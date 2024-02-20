from marshmallow import fields, Schema
from schema.parking_schema import ParkingSchema


class AggregatedParkingSchema(Schema):
    parking = fields.Nested(ParkingSchema)
    timestamp = fields.DateTime("iso")
    user_id = fields.Int()
