from unittest import TestCase
from chibi_filter import Chibi_filter
from chibi_filter_elasticsearch import descriptor
from chibi_filter import Q
from chibi_filter.exceptions import Filter_not_exits

from elasticsearch_dsl import Document, field


class Filter_numeric( Chibi_filter ):
    numeric_1 = descriptor.Numeric()
    numeric_2 = descriptor.Numeric()


class Filter_with_nested( Chibi_filter ):
    numerics = Filter_numeric().nested( 'numerics' )


class Filter_with_inner( Chibi_filter ):
    numerics = Filter_numeric().descriptor()


class Dump_elastic_model( Document ):
    numeric_1 = field.Integer()
    numeric_2 = field.Integer()


class Test_filter_elastic( TestCase ):
    def test_serialize_data( self ):
        q_1 = Q( numeric_1__eq=10 )
        q_2 = Q( numeric_1__gte=10 )
        q_3 = q_1 & q_2
        filter_instance = Filter_numeric( q_3.to_dict() )
        filter_instance.serialize_data()
        self.assertEqual( q_3.to_dict(), filter_instance.q.to_dict() )

    def test_process_q_leaf( self ):
        q_1 = Q( numeric_1__eq=10 )
        q_result = Filter_numeric.process_q_leaf( q_1 )
        expected = { 'term': { 'numeric_1': 10 } }
        self.assertEqual( q_result.to_dict(), expected )

    def test_process_q_node( self ):
        q_1 = Q( numeric_1__eq=10 )
        q_2 = Q( numeric_1__gte=10 )
        q_3 = q_1 & q_2
        q_result = Filter_numeric.process_q_node( q_3 )
        expected = {
            'bool': {
                'must': [
                    { 'term': { 'numeric_1': 10} },
                    { 'range': { 'numeric_1': { 'gte': 10 } } }
                ]
            }
        }
        self.assertDictEqual( q_result.to_dict(), expected )

    def test_filter_return_query_elastic( self ):
        q_1 = Q( numeric_1__eq=10 )
        q_2 = Q( numeric_1__gte=10 )
        q_3 = q_1 & q_2
        filter_instance = Filter_numeric(
            q_3.to_dict(), Dump_elastic_model.search() )

        query_result = filter_instance.doing_query()

        expected = {
            'query': {
                'bool': {
                    'must': [
                        { 'term': { 'numeric_1': 10} },
                        { 'range': { 'numeric_1': { 'gte': 10 } } }
                    ]
                }
            }
        }
        self.assertDictEqual( query_result.to_dict(), expected )

    def test_filter_with_inner_filters( self ):
        q_1 = Q( numerics__numeric_1__eq=10 )
        q_2 = Q( numerics__numeric_1__gte=10 )
        q_3 = q_1 & q_2
        filter_instance = Filter_with_inner(
            q_3.to_dict(), Dump_elastic_model.search() )

        query_result = filter_instance.doing_query()

        expected = {
            'query': {
                'bool': {
                    'must': [
                        { 'term': { 'numerics.numeric_1': 10} },
                        { 'range': { 'numerics.numeric_1': { 'gte': 10 } } }
                    ]
                }
            }
        }
        self.assertDictEqual( query_result.to_dict(), expected )


    def test_filter_with_nested_filters( self ):
        q_1 = Q( numerics__numeric_1__eq=10 )
        q_2 = Q( numerics__numeric_1__gte=10 )
        q_3 = q_1 & q_2
        filter_instance = Filter_with_nested(
            q_3.to_dict(), Dump_elastic_model.search() )

        query_result = filter_instance.doing_query()

        expected = {
            'query': {
                'bool': {
                    'must': [
                        {
                            'nested': {
                                'path': 'numerics',
                                'query': {
                                    'term': {
                                        'numerics.numeric_1': 10
                                    }
                                }
                            }
                        },
                        {
                            'nested': {
                                'path': 'numerics',
                                'query': {
                                    'range': {
                                        'numerics.numeric_1': {
                                            'gte': 10
                                        }
                                    }
                                }
                            }
                        }
                    ]
                }
            }
        }
        self.assertDictEqual( query_result.to_dict(), expected )

    def test_filter_not_exists( self ):
        q_1 = Q( fuck__eq=10 )
        q_2 = Q( stuff__eq=10 )
        q_3 = q_1 & q_2
        with self.assertRaises( Filter_not_exits ):
            Filter_with_inner( q_3.to_dict(), Dump_elastic_model.search() )

    def test_get_field_by_donkey_lvl_1( self ):
        filter_field = Filter_numeric.get_filter_by_donkey( 'numeric_1' )
        self.assertEqual( filter_field.name, 'numeric_1' )
        filter_field = Filter_numeric.get_filter_by_donkey( 'numeric_2' )
        self.assertEqual( filter_field.name, 'numeric_2' )

    def test_get_field_by_donkey_lvl_2( self ):
        filter_field = Filter_with_inner.get_filter_by_donkey( 'numerics__numeric_1' )
        self.assertEqual( filter_field.name, 'numeric_1' )
        filter_field = Filter_with_inner.get_filter_by_donkey( 'numerics__numeric_2' )
        self.assertEqual( filter_field.name, 'numeric_2' )

    def test_get_field_by_donkey_lvl_2_with_nested( self ):
        filter_field = Filter_with_nested.get_filter_by_donkey( 'numerics__numeric_1' )
        self.assertEqual( filter_field.name, 'numeric_1' )
        filter_field = Filter_with_nested.get_filter_by_donkey( 'numerics__numeric_2' )
        self.assertEqual( filter_field.name, 'numeric_2' )
