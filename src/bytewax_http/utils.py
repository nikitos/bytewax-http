import base64

from dateutil.parser import isoparse
from json import loads
from typing import Any

from cloudevents.core.base import BaseCloudEvent, EventFactory
from cloudevents.core.formats.json import JSONFormat
from cloudevents.core.spec import SPECVERSION_V0_3, SPECVERSION_V1_0


class BatchFormat(JSONFormat):
    def read(
        self,
        event_factory: EventFactory | None,
        data: str | bytes,
    ) -> BaseCloudEvent:
        """
        Read a CloudEvent from a JSON formatted byte string.

        Supports both v0.3 and v1.0 CloudEvents:
        - v0.3: Uses 'datacontentencoding' attribute with 'data' field
        - v1.0: Uses 'data_base64' field (no datacontentencoding)

        :param event_factory: A factory function to create CloudEvent instances.
                             If None, automatically detects version from 'specversion' field.
        :param data: The JSON formatted byte array.
        :return: The CloudEvent instance.
        """
        decoded_data: str
        if isinstance(data, bytes):
            decoded_data = data.decode('utf-8')
        else:
            decoded_data = data

        if isinstance(data, dict):
            event_attributes = data
        else:
            event_attributes = loads(decoded_data)

        # Auto-detect version if factory not provided
        if event_factory is None:
            from cloudevents.core.bindings.common import get_event_factory_for_version

            specversion = event_attributes.get('specversion', SPECVERSION_V1_0)
            event_factory = get_event_factory_for_version(specversion)

        if 'time' in event_attributes:
            event_attributes['time'] = isoparse(event_attributes['time'])

        # Handle data field based on version
        specversion = event_attributes.get('specversion', SPECVERSION_V1_0)
        event_data: dict[str, Any] | str | bytes | None = event_attributes.pop('data', None)

        # v0.3: Check for datacontentencoding attribute
        if specversion == SPECVERSION_V0_3 and 'datacontentencoding' in event_attributes:
            encoding = event_attributes.get('datacontentencoding', '').lower()
            if encoding == 'base64' and isinstance(event_data, str):
                # Decode base64 encoded data in v0.3
                event_data = base64.b64decode(event_data)

        # v1.0: Check for data_base64 field (when data is None)
        if event_data is None:
            event_data_base64 = event_attributes.pop('data_base64', None)
            if event_data_base64 is not None:
                event_data = base64.b64decode(event_data_base64)

        return event_factory(event_attributes, event_data)
