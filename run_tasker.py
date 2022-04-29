from tasker import DatabaseTasker


def run():
    db_tasker = DatabaseTasker()
    db_tasker.create_tables()


if __name__ == '__main__':
    run()
