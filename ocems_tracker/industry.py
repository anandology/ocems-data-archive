from __future__ import annotations
import dataclasses
from dataclasses import dataclass, field
import typing
import re


def column(**metadata):
    """symtactic sugar for dataclasses.field to make adding metadata easier.
    """
    return field(metadata=metadata)

_camel_case_pattern = re.compile(r'(?<!^)(?=[A-Z])')

# https://stackoverflow.com/a/1176023
def _to_snake_case(text):
    """Converts text from camelCase to snake_case.

        >>> _to_snake_case("industryType")
        'industry_type'
    """
    return _camel_case_pattern.sub('_', text).lower()

def process_data(data_object, result=None):
    if result is None:
        result = {}

    fields = dataclasses.fields(data_object.__class__)

    for f in fields:
        if f.metadata.get("ignore"):
            continue

        target = f.metadata.get('target') or _to_snake_case(f.name)
        value = getattr(data_object, f.name)
        if dataclasses.is_dataclass(value):
            process_data(value, result)
        else:
            result[target] = value

    return result

def dataclass_from_dict(klass, d):
    if klass == str:
        return d
    # print("dataclass_from_dict", repr(klass), dataclasses.is_dataclass(klass))
    if not dataclasses.is_dataclass(klass):
        return d

    # typing.get_type_hints resolves the names
    fieldtypes = typing.get_type_hints(klass)

    return klass(**{f:dataclass_from_dict(fieldtypes.get(f), d and d.get(f)) for f in fieldtypes})

@dataclass
class _IndustryZone:
    id: str = column(target="zone_id")
    name: str = column(target="zone_name")

@dataclass
class _IndustryState:
    id: str = column(target="state_id")
    name: str = column(target="state_name")
    zone: _IndustryZone
    isGangaBasin: str = column(target="state_is_ganga_basin")

@dataclass
class _IndustryType:
    id: str = column(target="industry_type_id")
    type: str = column(target="industry_type")
    description: str = column(target="industry_type_description")
    status: str = column(target="industry_type_status")

@dataclass
class _GangaSegment:
    id: str = column(target="ganga_segment_id")
    name: str = column(target="ganga_segment_name")

@dataclass
class Industry:
    id: str
    name: str
    status: str
    createdDate: str
    lastUpdateDate: str
    address: str
    latitude: str
    longitude: str
    city: str
    code: str
    zip: str
    timezone: str
    industryType: _IndustryType
    gangaSegment: _GangaSegment
    state: _IndustryState
    contactEmail: str = column(ignore=True)
    contactNo: str = column(ignore=True)
    consumerLastDataAt: str
    spcbRegionalOffice: str
    listOfEntities: str = column(ignore=True)
    gangaBasin: str
    isGangaBasin: str

    @classmethod
    def from_dict(cls, data):
        # print("-- from_dict", data)
        return dataclass_from_dict(cls, data)

    def to_flat_dict(self):
        return process_data(self)