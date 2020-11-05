from chibi_filter import descriptor
from chibi_filter_elasticsearch.lookup import (
    Number as Number_lookup,
    String as String_lookup,
    Datetime as Datetime_lookup,
    Boolean as Boolean_lookup,
)


class Numeric( descriptor.Numeric ):
    def get_lookup_class( self, key, lookup, value, nested_path=None ):
        self.evaluate_lookup( lookup )
        number_instance = Number_lookup( key, lookup, value, nested_path )
        return number_instance


class Datetime( descriptor.Datetime ):
    def get_lookup_class( self, key, lookup, value, nested_path=None ):
        self.evaluate_lookup( lookup )
        datetime_instance = Datetime_lookup(
            key, lookup, value, nested_path )
        return datetime_instance


class String( descriptor.String ):
    default = 'match'

    def get_lookup_class( self, key, lookup, value, nested_path=None ):
        self.evaluate_lookup( lookup )
        string_instance = String_lookup( key, lookup, value, nested_path )
        return string_instance


class Boolean( descriptor.Boolean ):
    def get_lookup_class( self, key, lookup, value, nested_path=None ):
        self.evaluate_lookup( lookup )
        string_instance = Boolean_lookup( key, lookup, value, nested_path )
        return string_instance
