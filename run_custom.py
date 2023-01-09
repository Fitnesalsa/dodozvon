from postgresql import Database


def run():
    """
    Скрипт делает три вещи:
    1. парсит промокоды из Додо ИС, с данными: пиццерии is_active + new_shop_exclude \ lost_shop_exclude, даты из
    promos_start_date, promos_end_date;
    2. делает excel файлы по спарcенным промо и выгружает на яндекс диск
    3. выгружает заказы за период promos_start_date, promos_end_date по пиццериям is_active на яндекс диск
    """

    db = Database()
    db.connect()




if __name__ == '__main__':  # явный запуск скрипта
    run()