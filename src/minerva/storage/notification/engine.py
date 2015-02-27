from minerva.storage import Engine


class NotificationEngine(Engine):
    @staticmethod
    def store(package):
        raise NotImplementedError()
