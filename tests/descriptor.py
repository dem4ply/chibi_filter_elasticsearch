from unittest import TestCase
from chibi_filter import Q
from chibi_filter_elasticsearch.descriptor import Numeric, String, Datetime, Boolean


class Test_numeric_leaf( TestCase ):
    def test_convert_q_eq( self ):
        field = Numeric()
        q = Q( field_numeric__eq=1 )
        convert_q = field.convert_q( q )
        result = convert_q.to_dict()
        expected = { 'term': { 'field_numeric': 1 } }
        self.assertEqual( result, expected )

    def test_convert_q_gt( self ):
        field = Numeric()
        q = Q( field_numeric__gt=1 )
        convert_q = field.convert_q( q )
        result = convert_q.to_dict()
        expected = { 'range': { 'field_numeric': { 'gt': 1 } } }
        self.assertEqual( result, expected )

    def test_convert_q_lt( self ):
        field = Numeric()
        q = Q( field_numeric__lt=1 )
        convert_q = field.convert_q( q )
        result = convert_q.to_dict()
        expected = { 'range': { 'field_numeric': { 'lt': 1 } } }
        self.assertEqual( result, expected )

    def test_convert_q_lte( self ):
        field = Numeric()
        q = Q( field_numeric__lte=1 )
        convert_q = field.convert_q( q )
        result = convert_q.to_dict()
        expected = { 'range': { 'field_numeric': { 'lte': 1 } } }
        self.assertEqual( result, expected )

    def test_convert_q_gte( self ):
        field = Numeric()
        q = Q( field_numeric__gte=1 )
        convert_q = field.convert_q( q )
        result = convert_q.to_dict()
        expected = { 'range': { 'field_numeric': { 'gte': 1 } } }
        self.assertEqual( result, expected )


class Test_string_leaf( TestCase ):
    def test_convert_q_eq( self ):
        field = String()
        q = Q( field_string__eq='asdf' )
        convert_q = field.convert_q( q )
        result = convert_q.to_dict()
        expected = { 'term': { 'field_string': 'asdf' } }
        self.assertEqual( result, expected )

    def test_convert_q_regex( self ):
        field = String()
        q = Q( field_string__regex=r'asdf.*x' )
        convert_q = field.convert_q( q )
        result = convert_q.to_dict()
        expected = { 'regexp': { 'field_string': r'asdf.*x' } }
        self.assertEqual( result, expected )

    def test_convert_q_in( self ):
        field = String()
        q = Q( field_string__in=[ 'asdf', 'zxcv' ] )
        convert_q = field.convert_q( q )
        result = convert_q.to_dict()
        expected = { 'terms': { 'field_string': [ 'asdf', 'zxcv' ] } }
        self.assertEqual( result, expected )

    def test_convert_q_contains( self ):
        field = String()
        q = Q( field_string__contain=[ 'asdf', 'zxcv' ] )
        convert_q = field.convert_q( q )
        result = convert_q.to_dict()
        expected = { 'terms': { 'field_string': [ 'asdf', 'zxcv' ] } }
        self.assertEqual( result, expected )


class Test_date_leaf( TestCase ):
    def test_convert_q_eq( self ):
        field = Datetime()
        q = Q( field_date__eq='now/d' )
        convert_q = field.convert_q( q )
        result = convert_q.to_dict()
        expected = { 'term': { 'field_date': 'now/d' } }
        self.assertEqual( result, expected )

    def test_convert_q_gt( self ):
        field = Datetime()
        q = Q( field_datetime__gt='now/d' )
        convert_q = field.convert_q( q )
        result = convert_q.to_dict()
        expected = { 'range': { 'field_datetime': { 'gt': 'now/d' } } }
        self.assertEqual( result, expected )

    def test_convert_q_lt( self ):
        field = Datetime()
        q = Q( field_datetime__lt='now/d' )
        convert_q = field.convert_q( q )
        result = convert_q.to_dict()
        expected = { 'range': { 'field_datetime': { 'lt': 'now/d' } } }
        self.assertEqual( result, expected )

    def test_convert_q_lte( self ):
        field = Datetime()
        q = Q( field_datetime__lte='now/d' )
        convert_q = field.convert_q( q )
        result = convert_q.to_dict()
        expected = { 'range': { 'field_datetime': { 'lte': 'now/d' } } }
        self.assertEqual( result, expected )

    def test_convert_q_gte( self ):
        field = Datetime()
        q = Q( field_datetime__gte='now/d' )
        convert_q = field.convert_q( q )
        result = convert_q.to_dict()
        expected = { 'range': { 'field_datetime': { 'gte': 'now/d' } } }
        self.assertEqual( result, expected )


class Test_boolean_leaf( TestCase ):
    def test_convert_q_eq_true( self ):
        field = Boolean()
        q = Q( field_date__eq='1' )
        convert_q = field.convert_q( q )
        result = convert_q.to_dict()
        expected = { 'term': { 'field_date': True } }
        self.assertEqual( result, expected )

    def test_convert_q_eq_false( self ):
        field = Boolean()
        q = Q( field_date__eq='' )
        convert_q = field.convert_q( q )
        result = convert_q.to_dict()
        expected = { 'term': { 'field_date': False } }
        self.assertEqual( result, expected )
