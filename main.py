import logging
from datetime import datetime, date
from aiogram import Bot, Router, types, Dispatcher
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.filters import CommandStart, Command, StateFilter, BaseFilter
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, ReplyKeyboardMarkup, ReplyKeyboardRemove
from aiogram import F
import aiomysql
from geopy.distance import geodesic
import asyncio

# Конфигурация
API_TOKEN = '7687732961:AAFsVoYrtr70LIuWaavd2g-TK0qu9HfKwXw'
DB_CONFIG = {
    'host': 'localhost',
    'user': 'root',
    'password': '888999000',
    'db': 'report_db',
    'port': 3306
}
MANAGER_ID = 7057936136  # Фиксированный ID руководителя

# Настройка логирования
logging.basicConfig(level=logging.INFO)

# Инициализация бота
bot = Bot(token=API_TOKEN)
storage = MemoryStorage()
dp = Router()

# Константы для пагинации
OBJECTS_PER_PAGE = 6

# Middleware для логирования обновлений
async def log_update_middleware(handler, update: types.Update, data: dict):
    user = None
    if update.message:
        user = update.message.from_user
    elif update.callback_query:
        user = update.callback_query.from_user
    elif update.inline_query:
        user = update.inline_query.from_user

    if user:
        logging.info(
            f"Update from user: id={user.id}, username={user.username}, "
            f"first_name={user.first_name}, last_name={user.last_name}"
        )
        await log_action(user.id, "update_received")
    else:
        logging.info("Update without user information")

    return await handler(update, data)

# Подключение к базе данных MySQL
async def get_db_connection():
    try:
        return await aiomysql.connect(**DB_CONFIG)
    except Exception as e:
        logging.error(f"Ошибка подключения к MySQL: {e}")
        raise

# Логирование действий в таблицу logs
async def log_action(user_id: int, action: str):
    conn = await get_db_connection()
    try:
        async with conn.cursor() as cursor:
            await cursor.execute(
                "INSERT INTO logs (user_id, action, timestamp) VALUES (%s, %s, NOW())",
                (user_id, action)
            )
            await conn.commit()
    except Exception as e:
        logging.error(f"Ошибка при логировании действия: {e}")
    finally:
        conn.close()

# Отслеживание сообщений бота
async def track_bot_message(message: types.Message, state: FSMContext):
    data = await state.get_data()
    bot_message_ids = data.get('bot_message_ids', [])
    bot_message_ids.append(message.message_id)
    await state.update_data(bot_message_ids=bot_message_ids)

# Отслеживание сообщений пользователя
async def track_user_message(message: types.Message, state: FSMContext):
    data = await state.get_data()
    user_message_ids = data.get('user_message_ids', [])
    user_message_ids.append(message.message_id)
    await state.update_data(user_message_ids=user_message_ids)

# Проверка роли пользователя (employee или MANAGER_ID)
async def check_role(user_id: int) -> bool:
    if user_id == MANAGER_ID:
        return True
    conn = await get_db_connection()
    try:
        async with conn.cursor() as cursor:
            await cursor.execute("SELECT role FROM users WHERE user_id = %s", (user_id,))
            user = await cursor.fetchone()
            if user:
                role = user[0].strip().lower()
                return role == 'employee'
            return False
    finally:
        conn.close()

# Получить роль пользователя
async def get_user_role(user_id: int) -> str:
    conn = await get_db_connection()
    try:
        async with conn.cursor() as cursor:
            await cursor.execute("SELECT role FROM users WHERE user_id = %s", (user_id,))
            user = await cursor.fetchone()
        return user[0] if user else None
    finally:
        conn.close()

# Классы для машины состояний
class Shift(StatesGroup):
    location = State()
    tasks = State()
    issues = State()

class ContactManager(StatesGroup):
    message = State()

class ManagerResponse(StatesGroup):
    waiting_response = State()

# Команда /start
@dp.message(CommandStart())
async def start(message: types.Message, state: FSMContext):
    await track_user_message(message, state)
    user_id = message.from_user.id
    role = await get_user_role(user_id)

    if role is None and user_id != MANAGER_ID:
        reply = await message.reply("Вас нет в базе данных. Обратитесь к администратору для регистрации.")
        await track_bot_message(reply, state)
        await log_action(user_id, "unregistered_access_attempt")
        return

    if role != 'employee' and user_id != MANAGER_ID:
        reply = await message.reply("Этот бот предназначен только для сотрудников.")
        await track_bot_message(reply, state)
        return

    if user_id == MANAGER_ID:
        reply = await message.reply("Вы авторизованы как руководитель. Ожидайте сообщений от сотрудников.")
    else:
        reply = await message.reply("Вы успешно авторизованы!")
    await track_bot_message(reply, state)
    if user_id != MANAGER_ID:
        await show_menu(user_id, message, state)
    await log_action(user_id, "started")

# Показать меню для сотрудника
async def show_menu(user_id: int, message: types.Message, state: FSMContext):
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Начать смену", callback_data="start_shift")],
        [InlineKeyboardButton(text="Посмотреть рейтинг", callback_data="view_rating")],
        [InlineKeyboardButton(text="Справка", callback_data="view_help")]
    ])
    reply = await bot.send_message(message.chat.id, "Выберите действие:", reply_markup=keyboard)
    await track_bot_message(reply, state)
    await log_action(user_id, "opened_menu")

# Команда /restart
@dp.message(Command("restart"))
async def cmd_restart(message: types.Message, state: FSMContext):
    await track_user_message(message, state)
    user_id = message.from_user.id
    if not await check_role(user_id):
        reply = await message.reply("Этот бот предназначен только для сотрудников и руководителя.")
        await track_bot_message(reply, state)
        return
    await state.clear()
    if user_id != MANAGER_ID:
        await show_menu(user_id, message, state)
    else:
        reply = await message.reply("Состояние сброшено. Ожидайте сообщений от сотрудников.")
        await track_bot_message(reply, state)
    await log_action(user_id, "restarted")

# Обработка нажатий на кнопки
@dp.callback_query(F.data == "start_shift")
async def start_shift_callback(callback_query: types.CallbackQuery, state: FSMContext):
    user_id = callback_query.from_user.id
    if not await check_role(user_id):
        reply = await callback_query.message.reply("Этот бот предназначен только для сотрудников и руководителя.")
        await track_bot_message(reply, state)
        return
    if user_id == MANAGER_ID:
        reply = await callback_query.message.reply("Руководитель не может начинать смену.")
        await track_bot_message(reply, state)
        return
    conn = await get_db_connection()
    try:
        async with conn.cursor() as cursor:
            await cursor.execute("SELECT shift_id FROM shifts WHERE user_id = %s AND status = 'active'", (user_id,))
            active_shift = await cursor.fetchone()
            if active_shift:
                reply = await callback_query.message.reply("У вас уже есть активная смена.")
                await track_bot_message(reply, state)
                await show_shift_menu(user_id, callback_query.message, state)
                return
    finally:
        conn.close()
    await show_objects(user_id, page=1, message=callback_query.message, state=state)
    await callback_query.answer()
    await log_action(user_id, "started_shift_selection")

# Пагинация объектов для выбора смены
async def show_objects(user_id: int, page: int = 1, message: types.Message = None,
                       callback_query: types.CallbackQuery = None, state: FSMContext = None):
    conn = await get_db_connection()
    try:
        async with conn.cursor() as cursor:
            offset = (page - 1) * OBJECTS_PER_PAGE
            await cursor.execute("SELECT project_id, project_name FROM projects LIMIT %s OFFSET %s",
                                 (OBJECTS_PER_PAGE + 1, offset))
            objects = await cursor.fetchall()
            if not objects:
                text = "Нет доступных объектов."
            else:
                text = f"Выберите объект (страница {page}):\n"
                keyboard = InlineKeyboardMarkup(inline_keyboard=[])
                for obj in objects[:OBJECTS_PER_PAGE]:
                    keyboard.inline_keyboard.append(
                        [InlineKeyboardButton(text=obj[1], callback_data=f"select_object_{obj[0]}")])
                if page > 1:
                    keyboard.inline_keyboard.append(
                        [InlineKeyboardButton(text="<< Назад", callback_data=f"objects_page_{page - 1}")])
                if len(objects) > OBJECTS_PER_PAGE:
                    keyboard.inline_keyboard.append(
                        [InlineKeyboardButton(text="Вперед >>", callback_data=f"objects_page_{page + 1}")])
                keyboard.inline_keyboard.append([InlineKeyboardButton(text="Отмена", callback_data="cancel_shift")])

                if message:
                    reply = await message.reply(text, reply_markup=keyboard)
                    await track_bot_message(reply, state)
                elif callback_query:
                    await callback_query.message.edit_text(text, reply_markup=keyboard)
    finally:
        conn.close()

@dp.callback_query(F.data.startswith("objects_page_"))
async def objects_pagination_callback(callback_query: types.CallbackQuery, state: FSMContext):
    page = int(callback_query.data.split("_")[2])
    user_id = callback_query.from_user.id
    await show_objects(user_id, page=page, callback_query=callback_query, state=state)
    await callback_query.answer()
    await log_action(user_id, f"viewed_objects_page_{page}")

@dp.callback_query(F.data == "cancel_shift")
async def cancel_shift_callback(callback_query: types.CallbackQuery, state: FSMContext):
    user_id = callback_query.from_user.id
    await state.clear()
    await show_menu(user_id, callback_query.message, state)
    await callback_query.answer()
    await log_action(user_id, "canceled_shift_selection")

# Выбор объекта для смены
@dp.callback_query(F.data.startswith("select_object_"))
async def select_object_callback(callback_query: types.CallbackQuery, state: FSMContext):
    object_id = int(callback_query.data.split("_")[2])
    user_id = callback_query.from_user.id
    await state.update_data(object_id=object_id)
    reply = await callback_query.message.reply(
        "Чтобы начать смену, включите геопозицию:\n"
        "1. Нажмите кнопку 'Поделиться местоположением' (📍) ниже.\n"
        "2. Выберите 'Транслировать мое местоположение'.\n"
        "3. Установите время на 8 часов.\n\n"
        "Это позволит боту отслеживать ваше местоположение без лишнего расхода батареи."
    )
    await track_bot_message(reply, state)
    await state.set_state(Shift.location)
    await callback_query.answer()
    await log_action(user_id, f"selected_object_{object_id}")

# Обработка начала трансляции
@dp.message(F.location, StateFilter(Shift.location))
async def process_location(message: types.Message, state: FSMContext):
    await track_user_message(message, state)
    user_id = message.from_user.id
    data = await state.get_data()
    object_id = data.get('object_id')

    if not object_id:
        reply = await message.reply("Ошибка: объект не выбран. Начните смену заново.")
        await track_bot_message(reply, state)
        await state.clear()
        await show_menu(user_id, message, state)
        return

    if message.location.live_period and message.location.live_period >= 8 * 3600:
        await state.update_data(location=message.location)
        start_time = datetime.now().strftime("%H:%M:%S")
        reply = await message.reply(
            f"Трансляция геопозиции началась успешно!\n"
            f"Время начала смены: {start_time}"
        )
        await track_bot_message(reply, state)
        conn = await get_db_connection()
        try:
            async with conn.cursor() as cursor:
                await cursor.execute(
                    "INSERT INTO shifts (user_id, object_id, start_time, status, has_deviation) "
                    "VALUES (%s, %s, NOW(), 'active', 0)",
                    (user_id, object_id)
                )
                await conn.commit()
        except Exception as e:
            logging.error(f"Ошибка при начале смены: {e}")
            reply = await message.reply("Произошла ошибка при начале смены. Попробуйте снова.")
            await track_bot_message(reply, state)
            await state.clear()
            await show_menu(user_id, message, state)
            return
        finally:
            conn.close()
        await show_shift_menu(user_id, message, state)
    else:
        reply = await message.reply(
            "Ошибка: геопозиция должна быть включена.\n"
            "Повторите попытку, выбрав 'Транслировать мое местоположение' на 8 часов."
        )
        await track_bot_message(reply, state)

# Обработка обновлений геопозиции
@dp.edited_message(F.location)
async def handle_location_update(edited_message: types.Message, state: FSMContext):
    user_id = edited_message.from_user.id
    conn = await get_db_connection()
    try:
        async with conn.cursor() as cursor:
            await cursor.execute(
                "SELECT shift_id, object_id FROM shifts WHERE user_id = %s AND status = 'active'",
                (user_id,)
            )
            shift = await cursor.fetchone()
            if not shift:
                logging.info(f"Геолокация от пользователя {user_id} проигнорирована: смена не активна")
                return

            shift_id, object_id = shift
            await cursor.execute(
                "SELECT latitude, longitude FROM projects WHERE project_id = %s",
                (object_id,)
            )
            project = await cursor.fetchone()
            if project:
                obj_lat, obj_lon = project
                emp_lat = edited_message.location.latitude
                emp_lon = edited_message.location.longitude
                distance = geodesic((obj_lat, obj_lon), (emp_lat, emp_lon)).meters
                logging.info(f"User {user_id} distance from object {object_id}: {distance:.2f} meters")
                await log_action(user_id, f"location_update_distance_{distance:.2f}")
                if distance > 200:
                    await cursor.execute(
                        "UPDATE shifts SET has_deviation = 1 WHERE shift_id = %s AND has_deviation = 0",
                        (shift_id,)
                    )
                    await conn.commit()

                if edited_message.location.live_period is None or edited_message.location.live_period == 0:
                    reply = await bot.send_message(
                        user_id,
                        "Ваша трансляция геопозиции завершена. Пожалуйста, продлите её на 8 часов, чтобы продолжить смену."
                    )
                    await track_bot_message(reply, state)
                    await log_action(user_id, "live_location_ended")
    except Exception as e:
        logging.error(f"Ошибка при обработке геопозиции: {e}")
    finally:
        conn.close()

# Меню во время смены
async def show_shift_menu(user_id: int, message: types.Message, state: FSMContext):
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Связаться с руководителем", callback_data="contact_manager")],
        [InlineKeyboardButton(text="Завершить смену", callback_data="end_shift")]
    ])
    reply = await message.reply("Ваша смена активна. Выберите действие:", reply_markup=keyboard)
    await track_bot_message(reply, state)

@dp.callback_query(F.data == "contact_manager")
async def contact_manager_callback(callback_query: types.CallbackQuery, state: FSMContext):
    user_id = callback_query.from_user.id
    if not await check_role(user_id):
        reply = await callback_query.message.reply("Этот бот предназначен только для сотрудников и руководителя.")
        await track_bot_message(reply, state)
        return
    if user_id == MANAGER_ID:
        reply = await callback_query.message.reply("Руководитель не может отправлять сообщения самому себе.")
        await track_bot_message(reply, state)
        return
    reply = await callback_query.message.reply("Опишите вашу проблему или вопрос для руководителя:")
    await track_bot_message(reply, state)
    await state.set_state(ContactManager.message)
    await callback_query.answer()
    await log_action(user_id, "initiated_contact_manager")

@dp.message(StateFilter(ContactManager.message))
async def process_contact_message(message: types.Message, state: FSMContext):
    await track_user_message(message, state)
    user_id = message.from_user.id
    contact_message = message.text

    conn = await get_db_connection()
    try:
        async with conn.cursor() as cursor:
            await cursor.execute(
                "SELECT object_id FROM shifts WHERE user_id = %s AND status = 'active'",
                (user_id,)
            )
            shift = await cursor.fetchone()
            object_id = shift[0] if shift else "Не указан"
    finally:
        conn.close()

    try:
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="Ответить", callback_data=f"reply_to_{user_id}")]
        ])
        await bot.send_message(
            MANAGER_ID,
            f"Сообщение от сотрудника {user_id} (объект: {object_id}):\n{contact_message}",
            reply_markup=keyboard
        )
        reply = await message.reply("Сообщение отправлено руководителю. Ожидайте ответа.")
        await track_bot_message(reply, state)
    except Exception as e:
        logging.error(f"Ошибка при отправке сообщения руководителю: {e}")
        reply = await message.reply("Ошибка при отправке. Попробуйте снова.")
        await track_bot_message(reply, state)
    await state.clear()
    await show_shift_menu(user_id, message, state)

@dp.callback_query(F.data.startswith("reply_to_"))
async def manager_reply_callback(callback_query: types.CallbackQuery, state: FSMContext):
    if callback_query.from_user.id != MANAGER_ID:
        await callback_query.message.reply("Только руководитель может отвечать.")
        return
    employee_id = int(callback_query.data.split("_")[2])
    reply = await callback_query.message.reply("Введите ваш ответ сотруднику:")
    await track_bot_message(reply, state)
    await state.update_data(employee_id=employee_id)
    await state.set_state(ManagerResponse.waiting_response)
    await callback_query.answer()

@dp.message(StateFilter(ManagerResponse.waiting_response))
async def process_manager_response(message: types.Message, state: FSMContext):
    if message.from_user.id != MANAGER_ID:
        return
    data = await state.get_data()
    employee_id = data.get('employee_id')
    if not employee_id:
        reply = await message.reply("Ошибка: не найден ID сотрудника.")
        await track_bot_message(reply, state)
        await state.clear()
        return
    try:
        await bot.send_message(employee_id, f"Ответ от руководителя:\n{message.text}")
        reply = await message.reply("Ответ отправлен сотруднику.")
        await track_bot_message(reply, state)
    except Exception as e:
        logging.error(f"Ошибка при отправке ответа: {e}")
        reply = await message.reply("Ошибка при отправке ответа.")
        await track_bot_message(reply, state)
    await state.clear()

@dp.callback_query(F.data == "end_shift")
async def end_shift_callback(callback_query: types.CallbackQuery, state: FSMContext):
    user_id = callback_query.from_user.id
    if not await check_role(user_id):
        reply = await callback_query.message.reply("Этот бот предназначен только для сотрудников и руководителя.")
        await track_bot_message(reply, state)
        return
    if user_id == MANAGER_ID:
        reply = await callback_query.message.reply("Руководитель не может завершать смену.")
        await track_bot_message(reply, state)
        return
    reply = await callback_query.message.reply(
        "Опишите, какие задачи вы выполнили за смену:"
    )
    await track_bot_message(reply, state)
    await state.set_state(Shift.tasks)
    await callback_query.answer()
    await log_action(user_id, "initiated_shift_end")

@dp.message(StateFilter(Shift.tasks))
async def process_tasks(message: types.Message, state: FSMContext):
    await track_user_message(message, state)
    await state.update_data(tasks=message.text)
    reply = await message.reply(
        "Возникли ли какие-либо сложности или отклонения в ходе работы?"
    )
    await track_bot_message(reply, state)
    await state.set_state(Shift.issues)
    await log_action(message.from_user.id, "entered_tasks")

# Улучшенная функция для очистки всех сообщений в чате
async def clear_chat(chat_id: int, last_message_id: int):
    logging.info(f"Начинаем очистку чата {chat_id} до сообщения {last_message_id}")
    message_id = last_message_id
    deleted_count = 0
    while message_id > 0:
        try:
            await bot.delete_message(chat_id, message_id)
            deleted_count += 1
            logging.info(f"Удалено сообщение {message_id} в чате {chat_id}")
            await asyncio.sleep(0.05)
        except Exception as e:
            if "message to delete not found" in str(e):
                logging.info(f"Сообщение {message_id} не найдено, продолжаем")
            elif "message can't be deleted" in str(e):
                logging.warning(f"Сообщение {message_id} нельзя удалить (возможно, старше 48 часов или нет прав)")
            else:
                logging.error(f"Ошибка при удалении сообщения {message_id}: {e}")
            message_id -= 1
            continue
        message_id -= 1
    logging.info(f"Очистка чата {chat_id} завершена, удалено {deleted_count} сообщений")
    return deleted_count

@dp.message(StateFilter(Shift.issues))
async def process_issues(message: types.Message, state: FSMContext):
    await track_user_message(message, state)
    user_id = message.from_user.id
    issues = message.text
    data = await state.get_data()
    tasks = data.get('tasks')
    object_id = data.get('object_id')

    if not tasks or not issues:
        reply = await message.reply("Ошибка: данные о задачах или проблемах отсутствуют. Начните смену заново.")
        await track_bot_message(reply, state)
        await state.clear()
        await show_menu(user_id, message, state)
        await log_action(user_id, "error_missing_tasks_or_issues")
        return

    if not object_id:
        conn = await get_db_connection()
        try:
            async with conn.cursor() as cursor:
                await cursor.execute(
                    "SELECT object_id FROM shifts WHERE user_id = %s AND status = 'active'",
                    (user_id,)
                )
                shift = await cursor.fetchone()
                if shift:
                    object_id = shift[0]
                else:
                    reply = await message.reply("Ошибка: активная смена не найдена. Начните смену заново.")
                    await track_bot_message(reply, state)
                    await state.clear()
                    await show_menu(user_id, message, state)
                    await log_action(user_id, "error_shift_not_found")
                    return
        finally:
            conn.close()

    conn = await get_db_connection()
    try:
        async with conn.cursor() as cursor:
            await cursor.execute(
                "SELECT shift_id, start_time, has_deviation FROM shifts WHERE user_id = %s AND status = 'active'",
                (user_id,)
            )
            shift = await cursor.fetchone()

            if not shift:
                reply = await message.reply("Ошибка: активная смена не найдена. Начните смену заново.")
                await track_bot_message(reply, state)
                await state.clear()
                await show_menu(user_id, message, state)
                await log_action(user_id, "error_shift_not_found")
                return

            shift_id, start_time, has_deviation = shift
            end_time = datetime.now()

            await cursor.execute(
                "UPDATE shifts SET end_time = %s, status = 'completed' WHERE shift_id = %s",
                (end_time, shift_id)
            )
            await cursor.execute(
                "INSERT INTO reports (user_id, project_id, report_date, tasks_completed, issues, approved, deviation, start_time, end_time) "
                "VALUES (%s, %s, %s, %s, %s, 0, %s, %s, %s)",
                (user_id, object_id, date.today(), tasks, issues, has_deviation, start_time, end_time)
            )
            report_id = cursor.lastrowid  # Получаем ID созданного отчета
            await conn.commit()

            # Отправка отчета менеджеру
            deviation_text = " (отклонение от объекта более 200 метров зафиксировано)" if has_deviation else ""
            report_text = (
                f"Новый отчет от сотрудника {user_id}:\n"
                f"Объект: {object_id}\n"
                f"Дата: {date.today()}\n"
                f"Начало: {start_time.strftime('%H:%M:%S')}\n"
                f"Конец: {end_time.strftime('%H:%M:%S')}\n"
                f"Задачи: {tasks}\n"
                f"Проблемы: {issues}{deviation_text}\n\n"
                f"Одобрить отчет? (ID: {report_id})"
            )
            keyboard = InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="Одобрить", callback_data=f"approve_report_{report_id}")],
                [InlineKeyboardButton(text="Отклонить", callback_data=f"reject_report_{report_id}")]
            ])
            await bot.send_message(MANAGER_ID, report_text, reply_markup=keyboard)

            # Сообщение сотруднику
            reply = await message.reply(
                f"Смена завершена. Время завершения: {end_time.strftime('%H:%M:%S')}{deviation_text}\n"
                f"Отчет отправлен руководителю на одобрение.\n"
                f"Пожалуйста, остановите трансляцию геолокации вручную в Telegram.\n"
                f"Чат будет очищен через несколько секунд."
            )
            last_message_id = reply.message_id
            await track_bot_message(reply, state)

            await asyncio.sleep(2)
            await clear_chat(message.chat.id, last_message_id)
            await state.clear()
            await show_menu(user_id, message, state)

    except Exception as e:
        logging.error(f"Ошибка при завершении смены: {e}")
        reply = await message.reply("Произошла ошибка при завершении смены. Попробуйте снова.")
        await track_bot_message(reply, state)
        await state.clear()
        await show_menu(user_id, message, state)
    finally:
        conn.close()

@dp.callback_query(F.data.startswith("approve_report_"))
async def approve_report_callback(callback_query: types.CallbackQuery, state: FSMContext):
    if callback_query.from_user.id != MANAGER_ID:
        await callback_query.message.reply("Только руководитель может одобрять отчеты.")
        return
    report_id = int(callback_query.data.split("_")[2])
    conn = await get_db_connection()
    try:
        async with conn.cursor() as cursor:
            await cursor.execute("UPDATE reports SET approved = 1 WHERE report_id = %s", (report_id,))
            await conn.commit()
        await callback_query.message.edit_text(callback_query.message.text + "\n\n✅ Отчет одобрен.")
        await cursor.execute("SELECT user_id FROM reports WHERE report_id = %s", (report_id,))
        user_id = (await cursor.fetchone())[0]
        await bot.send_message(user_id, "Ваш отчет одобрен руководителем!")
    except Exception as e:
        logging.error(f"Ошибка при одобрении отчета: {e}")
        await callback_query.message.reply("Ошибка при одобрении отчета.")
    finally:
        conn.close()
    await callback_query.answer()

@dp.callback_query(F.data.startswith("reject_report_"))
async def reject_report_callback(callback_query: types.CallbackQuery, state: FSMContext):
    if callback_query.from_user.id != MANAGER_ID:
        await callback_query.message.reply("Только руководитель может отклонять отчеты.")
        return
    report_id = int(callback_query.data.split("_")[2])
    conn = await get_db_connection()
    try:
        async with conn.cursor() as cursor:
            await cursor.execute("UPDATE reports SET approved = 0 WHERE report_id = %s", (report_id,))
            await conn.commit()
        await callback_query.message.edit_text(callback_query.message.text + "\n\n❌ Отчет отклонен.")
        await cursor.execute("SELECT user_id FROM reports WHERE report_id = %s", (report_id,))
        user_id = (await cursor.fetchone())[0]
        await bot.send_message(user_id, "Ваш отчет отклонен руководителем. Свяжитесь с ним для уточнений.")
    except Exception as e:
        logging.error(f"Ошибка при отклонении отчета: {e}")
        await callback_query.message.reply("Ошибка при отклонении отчета.")
    finally:
        conn.close()
    await callback_query.answer()

# Кнопка "Посмотреть рейтинг"
@dp.callback_query(F.data == "view_rating")
async def view_rating_callback(callback_query: types.CallbackQuery, state: FSMContext):
    user_id = callback_query.from_user.id
    if not await check_role(user_id):
        reply = await callback_query.message.reply("Этот бот предназначен только для сотрудников и руководителя.")
        await track_bot_message(reply, state)
        return
    if user_id == MANAGER_ID:
        reply = await callback_query.message.reply("Рейтинг доступен только для сотрудников.")
        await track_bot_message(reply, state)
        return
    conn = await get_db_connection()
    try:
        async with conn.cursor() as cursor:
            await cursor.execute(
                "SELECT user_id, name, points FROM users WHERE role = 'employee' ORDER BY points DESC LIMIT 10")
            users = await cursor.fetchall()
            rating = "Рейтинг сотрудников:\n"
            if not users:
                rating += "Нет сотрудников в рейтинге."
            else:
                for i, (user_id, name, points) in enumerate(users, 1):
                    rating += f"{i}. {name} (ID: {user_id}): {points} баллов\n"
            reply = await callback_query.message.reply(rating)
            await track_bot_message(reply, state)
    finally:
        conn.close()
    await callback_query.answer()

# Кнопка "Справка"
@dp.callback_query(F.data == "view_help")
async def view_help_callback(callback_query: types.CallbackQuery, state: FSMContext):
    user_id = callback_query.from_user.id
    if not await check_role(user_id):
        reply = await callback_query.message.reply("Этот бот предназначен только для сотрудников и руководителя.")
        await track_bot_message(reply, state)
        return
    if user_id == MANAGER_ID:
        help_text = (
            "Вы руководитель. Ваши возможности:\n"
            "- Отвечать на сообщения сотрудников.\n"
            "- Одобрять/отклонять отчеты."
        )
    else:
        help_text = (
            "Ваши возможности:\n"
            "- Начать смену\n"
            "- Посмотреть рейтинг\n"
            "- Связаться с руководителем\n"
            "- Завершить смену"
        )
    reply = await callback_query.message.reply(help_text)
    await track_bot_message(reply, state)
    await callback_query.answer()

# Обработчик неизвестных сообщений
@dp.message()
async def handle_unknown_message(message: types.Message, state: FSMContext):
    await track_user_message(message, state)
    current_state = await state.get_state()
    if current_state is not None:
        return
    user_id = message.from_user.id
    role = await get_user_role(user_id)
    if role is None and user_id != MANAGER_ID:
        reply = await message.reply("Вас нет в базе данных. Обратитесь к администратору для регистрации.")
        await track_bot_message(reply, state)
    elif role != 'employee' and user_id != MANAGER_ID:
        reply = await message.reply("Этот бот предназначен только для сотрудников и руководителя.")
        await track_bot_message(reply, state)
    elif user_id == MANAGER_ID:
        reply = await message.reply("Ожидайте сообщений от сотрудников или используйте кнопки для взаимодействия.")
        await track_bot_message(reply, state)
    else:
        reply = await message.reply("Неизвестная команда. Используйте /start, чтобы открыть меню.")
        await track_bot_message(reply, state)
    await log_action(user_id, "sent_unknown_message")

# Корректное завершение бота
async def shutdown():
    logging.info("Получен сигнал завершения. Останавливаем бота...")
    await bot.session.close()
    tasks = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
    [task.cancel() for task in tasks]
    await asyncio.gather(*tasks, return_exceptions=True)

# Запуск бота
async def main():
    dispatcher = Dispatcher(bot=bot, storage=storage)

    dispatcher.update.outer_middleware(log_update_middleware)
    dispatcher.include_router(dp)

    try:
        await dispatcher.start_polling(bot)
    except KeyboardInterrupt:
        await shutdown()
        logging.info("Бот остановлен пользователем.")
    finally:
        if not bot.session.closed:
            await bot.session.close()

if __name__ == '__main__':
    asyncio.run(main())