#from .base import Lookup as Lookup_baseJKUe
from chibi_filter.lookup import Lookup as Lookup_base
from elasticsearch_dsl import Q


class Lookup( Lookup_base ):
    def needed_transform_to_nested( self, q ):
        if self.nested_path is not None:
            q = Q( 'nested', path=self.nested_path, query=q.to_dict() )
        return q


class Number( Lookup ):
    def build( self ):
        if self.lookup == 'eq':
            q = Q( 'term', **{ self.field: self.value } )
        elif self.lookup in ( 'gt', 'gte', 'lt', 'lte' ):
            q = Q( 'range', **{ self.field: { self.lookup: self.value } }  )
        else:
            raise NotImplementedError(
                ( "the lookup {} is not implemented" ).format( self.lookup ) )
        return self.needed_transform_to_nested( q )


class String( Lookup ):
    def build( self ):
        if self.lookup == 'eq':
            q = Q( 'term', **{ self.field: self.value } )
        elif self.lookup == 'regex':
            q = Q( 'regexp', **{ self.field: self.value } )
        elif self.lookup in ( 'contain', 'in' ):
            q = Q( 'terms', **{ self.field: self.value } )
        elif self.lookup in ( 'match' ):
            q = Q( 'match', **{ self.field: self.value } )
        else:
            raise NotImplementedError(
                ( "the lookup {} is not implemented" ).format( self.lookup ) )
        return self.needed_transform_to_nested( q )

class Boolean( Lookup ):
    def build( self ):
        if self.lookup == 'eq':
            q = Q( 'term', **{ self.field: bool( self.value ) } )
        else:
            raise NotImplementedError(
                ( "the lookup {} is not implemented" ).format( self.lookup ) )
        return self.needed_transform_to_nested( q )

class Datetime( Lookup ):
    def build( self ):
        if self.lookup == 'eq':
            q = Q( 'term', **{ self.field: self.value } )
        elif self.lookup in ( 'gt', 'gte', 'lt', 'lte' ):
            q = Q( 'range', **{ self.field: { self.lookup: self.value } }  )
        else:
            raise NotImplementedError(
                ( "the lookup {} is not implemented" ).format( self.lookup ) )
        return self.needed_transform_to_nested( q )
