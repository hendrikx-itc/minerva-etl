from minerva.storage import Engine


class NotificationEngine(Engine):
    @staticmethod
    def store_cmd(package):
        raise NotImplementedError()
