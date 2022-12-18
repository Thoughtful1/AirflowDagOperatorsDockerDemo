from airflow.sensors.filesystem import FileSensor
from airflow.utils.decorators import apply_defaults
from typing import Any


class SmartFileSensor(FileSensor):
    poke_context_fields = ('filepath', 'fs_conn_id')

    @apply_defaults
    def __init__(self, **kwargs: Any):
        super().__init__(**kwargs)

    def is_smart_sensor_compatible(self):
        result = (not self.soft_fail and super().is_smart_sensor_compatible())
        return result
