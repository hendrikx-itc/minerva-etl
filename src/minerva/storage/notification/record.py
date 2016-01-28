

class Record:
    """Wraps all data for one notification."""

    def __init__(self, entity_ref, timestamp, attribute_names, values):
        self.entity_ref = entity_ref
        self.timestamp = timestamp
        self.attribute_names = attribute_names
        self.values = values
