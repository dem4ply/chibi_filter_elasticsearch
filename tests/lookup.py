from unittest import TestCase
from chibi_filter_elasticsearch.lookup import Number, String, Datetime, Boolean


class Test_number( TestCase ):
    def test_init( self ):
        lookup = Number( 'some_field', 'eq', 'nothign' )
        return lookup

    def test_build_eq( self ):
        lookup = self.test_init()
        q = lookup.build()
        expected = { 'term': { 'some_field': 'nothign' } }
        self.assertEqual( q.to_dict(), expected )

    def test_build_gt( self ):
        lookup = Number( 'some_field', 'gt', 1 )
        q = lookup.build()
        expected = { 'range': { 'some_field': { 'gt': 1 } } }
        self.assertEqual( q.to_dict(), expected )

    def test_build_gte( self ):
        lookup = Number( 'some_field', 'gte', 1 )
        q = lookup.build()
        expected = { 'range': { 'some_field': { 'gte': 1 } } }
        self.assertEqual( q.to_dict(), expected )

    def test_build_lt( self ):
        lookup = Number( 'some_field', 'lt', 1 )
        q = lookup.build()
        expected = { 'range': { 'some_field': { 'lt': 1 } } }
        self.assertEqual( q.to_dict(), expected )

    def test_build_lte( self ):
        lookup = Number( 'some_field', 'lte', 1 )
        q = lookup.build()
        expected = { 'range': { 'some_field': { 'lte': 1 } } }
        self.assertEqual( q.to_dict(), expected )

    def test_build_send_a_lookup_not_valid( self ):
        lookup = Number( 'some_field', 'asdf', 1 )
        with self.assertRaises( NotImplementedError ):
            lookup.build()


class Test_string( TestCase ):
    def test_init( self ):
        lookup = String( 'some_field', 'eq', 'nothign' )
        return lookup

    def test_build_eq( self ):
        lookup = self.test_init()
        q = lookup.build()
        expected = { 'term': { 'some_field': 'nothign' } }
        self.assertEqual( q.to_dict(), expected )

    def test_build_regex( self ):
        lookup = String( 'some_field', 'regex', r'asdf.*asdf' )
        q = lookup.build()
        expected = { 'regexp': { 'some_field': 'asdf.*asdf' } }
        self.assertEqual( q.to_dict(), expected )

    def test_build_in( self ):
        lookup = String( 'some_field', 'in', [ 'asdf', 'qwer' ] )
        q = lookup.build()
        expected = { 'terms': { 'some_field': [ 'asdf', 'qwer' ] } }
        self.assertEqual( q.to_dict(), expected )

    def test_build_contain( self ):
        lookup = String( 'some_field', 'contain', [ 'asdf', 'qwer' ] )
        q = lookup.build()
        expected = { 'terms': { 'some_field': [ 'asdf', 'qwer' ] } }
        self.assertEqual( q.to_dict(), expected )

    def test_build_send_a_lookup_not_valid( self ):
        lookup = String( 'some_field', 'asdf', 1 )
        with self.assertRaises( NotImplementedError ):
            lookup.build()


class Test_datetime( TestCase ):
    def test_init( self ):
        lookup = Datetime( 'some_field', 'eq', 'nothign' )
        return lookup

    def test_build_eq( self ):
        lookup = self.test_init()
        q = lookup.build()
        expected = { 'term': { 'some_field': 'nothign' } }
        self.assertEqual( q.to_dict(), expected )

    def test_build_gt( self ):
        lookup = Datetime( 'some_field', 'gt', 'now/d' )
        q = lookup.build()
        expected = { 'range': { 'some_field': { 'gt': 'now/d' } } }
        self.assertEqual( q.to_dict(), expected )

    def test_build_gte( self ):
        lookup = Datetime( 'some_field', 'gte', 'now/d' )
        q = lookup.build()
        expected = { 'range': { 'some_field': { 'gte': 'now/d' } } }
        self.assertEqual( q.to_dict(), expected )

    def test_build_lt( self ):
        lookup = Datetime( 'some_field', 'lt', 'now/d' )
        q = lookup.build()
        expected = { 'range': { 'some_field': { 'lt': 'now/d' } } }
        self.assertEqual( q.to_dict(), expected )

    def test_build_lte( self ):
        lookup = Datetime( 'some_field', 'lte', 'now/d' )
        q = lookup.build()
        expected = { 'range': { 'some_field': { 'lte': 'now/d' } } }
        self.assertEqual( q.to_dict(), expected )

    def test_build_send_a_lookup_not_valid( self ):
        lookup = Datetime( 'some_field', 'asdf', 1 )
        with self.assertRaises( NotImplementedError ):
            lookup.build()


class Test_boolean( TestCase ):
    def test_init( self ):
        lookup = Boolean( 'some_field', 'eq', 'nothign' )
        return lookup

    def test_build_eq( self ):
        lookup = self.test_init()
        q = lookup.build()
        expected = { 'term': { 'some_field': True } }
        self.assertEqual( q.to_dict(), expected )

    def test_build_send_a_lookup_not_valid( self ):
        lookup = Datetime( 'some_field', 'asdf', 1 )
        with self.assertRaises( NotImplementedError ):
            lookup.build()
