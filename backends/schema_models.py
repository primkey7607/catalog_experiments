"""
Schema Models are core data structure of Catalog Service. They will be mapped to runtime-instances by ORM
"""

from peewee import *
from playhouse.sqlite_ext import *

## Schema Models

database = SqliteDatabase(None)

class BaseModel(Model):
    class Meta:
        database = database

######### Normalized Schema ###################
#TODO: remove the Item table! We made up our minds that we would
#replicate the Item attributes across tables 
#(making the itemId the primary key of every table)
# class Item(BaseModel):
#     version = IntegerField()
#     timestamp = DateTimeField()
#     user = DeferredForeignKey('User', null=True)


class UserType(BaseModel):
    name = TextField()
    description = TextField()


class User(BaseModel):
    name = TextField()
    user_type = ForeignKeyField(UserType, backref='user_type')
    schema = JSONField()
    version = IntegerField()
    timestamp = DateTimeField()
    user = DeferredForeignKey('User', null=True)


class AssetType(BaseModel):
    name = TextField()
    description = TextField()


class Asset(BaseModel):
    name = TextField()
    asset_type = ForeignKeyField(AssetType, backref='asset_type', null=True)
    version = IntegerField()
    timestamp = DateTimeField()
    user = ForeignKeyField(User, null=True)


class WhoProfile(BaseModel):
    version = IntegerField()
    timestamp = DateTimeField()
    write_user = ForeignKeyField(User, null=True)
    asset = ForeignKeyField(Asset, backref='who_asset')
    user = ForeignKeyField(User, backref='who_user', null=True)
    schema = JSONField()


class WhatProfile(BaseModel):
    version = IntegerField()
    timestamp = DateTimeField()
    user = ForeignKeyField(User, null=True)
    asset = ForeignKeyField(Asset, backref='what_asset')
    schema = JSONField()


class HowProfile(BaseModel):
    version = IntegerField()
    timestamp = DateTimeField()
    user = ForeignKeyField(User, null=True)
    asset = ForeignKeyField(Asset, backref='how_asset')
    schema = JSONField()


class WhyProfile(BaseModel):
    version = IntegerField()
    timestamp = DateTimeField()
    user = ForeignKeyField(User, null=True)
    asset = ForeignKeyField(Asset, backref='why_asset')
    schema = JSONField()


class WhenProfile(BaseModel):
    version = IntegerField()
    timestamp = DateTimeField()
    user = ForeignKeyField(User, null=True)
    asset = ForeignKeyField(Asset, backref='when_asset')
    asset_timestamp = DateTimeField(null=True)
    expiry_date = DateTimeField(null=True)
    start_date = DateTimeField(null=True)


class SourceType(BaseModel):
    connector = TextField()
    serde = TextField()
    datamodel = TextField()


class Source(BaseModel):
    version = IntegerField()
    timestamp = DateTimeField()
    user = ForeignKeyField(User, null=True)
    name = TextField()
    source_type = ForeignKeyField(SourceType, backref='source_type')
    schema = JSONField()


class WhereProfile(BaseModel):
    version = IntegerField()
    timestamp = DateTimeField()
    user = ForeignKeyField(User, null=True)
    asset = ForeignKeyField(Asset, backref='where_asset')
    access_path = TextField(null=True)
    source = ForeignKeyField(Source, backref='where_source', null=True)
    configuration = JSONField(null=True)


class Action(BaseModel):
    version = IntegerField()
    timestamp = DateTimeField()
    user = ForeignKeyField(User, null=True)
    asset = ForeignKeyField(Asset, backref='action_asset')
    who = ForeignKeyField(WhoProfile, backref='action_who')
    how = ForeignKeyField(HowProfile, backref='action_how')
    why = ForeignKeyField(WhyProfile, backref='action_why')
    when = ForeignKeyField(WhenProfile, backref='action_when')
    

class RelationshipType(BaseModel):
    name = TextField()
    description = TextField()

class Relationship(BaseModel):
    version = IntegerField()
    timestamp = DateTimeField()
    user = ForeignKeyField(User, null=True)
    relationship_type = ForeignKeyField(RelationshipType, backref='rel_type')
    schema = JSONField()

#the many-many relationship table for the 
#'relationship-to-asset' relationship
class Asset_Relationships(BaseModel):
    asset = ForeignKeyField(Asset, backref='rel_asset')
    relationship = ForeignKeyField(Relationship, backref='asset_rel')

######### End Normalized Schema ###################

######### Data Vault Schema ###################
class H_User(BaseModel):
    #NOT a primary key, but uniquely identifies
    #a user. The primary key is the "item id", which identifies
    #a specific version of the user
    #(we need this because if this is a true append-only database,
    #then if a user makes a mistake inserting into the database,
    # then when they reinsert, we'll have a new version.)
    #UPDATE: hubs don't need their own ID fields (besides "itemID"), because
    #the entire point of data vault is that the satellites will take care of
    #evolution for you--you shouldn't need to make a new ID for an entity
    #that's already being tracked in your database.
    #userId = IntegerField()
    name = TextField()
    
    version = IntegerField()
    timestamp = DateTimeField()
    #it doesn't make sense to have a user here, because if a user
    #inserts their own name, that user 
    #obviously doesn't exist in the database

class S_User_schema(BaseModel):
    user = ForeignKeyField(H_User, backref='h_schema_user')
    schema = JSONField()
    
    version = IntegerField()
    timestamp = DateTimeField()

#name and description shouldn't be part of a hub table
#TODO: come back and fix above
class H_UserType(BaseModel):
    #user_typeId = IntegerField()
    
    version = IntegerField()
    timestamp = DateTimeField()

class S_UserTypeAttributes(BaseModel):
    name = TextField()
    description = TextField()
    
    version = IntegerField()
    timestamp = DateTimeField()

class L_UserTypeLink(BaseModel):
    user = ForeignKeyField(H_User, backref='l_user')
    user_type = ForeignKeyField(H_UserType, backref='l_type')

class H_Asset(BaseModel):
    #NOT a primary key, but uniquely identifies
    #an asset. The primary key is the "item id", which identifies
    #a specific version of the asset
    assetId = IntegerField()
    name = TextField()
    
    version = IntegerField()
    timestamp = DateTimeField()
    user = ForeignKeyField(H_User, backref='h_asset_user')

class H_AssetType(BaseModel):
    asset_typeId = IntegerField()
    
    version = IntegerField()
    timestamp = DateTimeField()
    user = ForeignKeyField(H_User, backref='h_atype_user')
    
class S_AssetTypeAttributes(BaseModel):
    name = TextField()
    description = TextField()
    
    version = IntegerField()
    timestamp = DateTimeField()
    user = ForeignKeyField(H_User, backref='s_atype_user')
    

class L_AssetTypeLink(BaseModel):
    asset = ForeignKeyField(H_Asset, backref='l_type_asset')
    asset_type = ForeignKeyField(H_AssetType, backref='l_asset_type')
    
    version = IntegerField()
    timestamp = DateTimeField()
    user = ForeignKeyField(H_User, backref='l_atype_user')


class H_WhoProfile(BaseModel):
     #NOT a primary key, but uniquely identifies
    #some version of a who-profile for an asset. The primary key is the "item id", which identifies
    #a specific version of the asset
    who_profileId = IntegerField()
    
    version = IntegerField()
    timestamp = DateTimeField()
    user = ForeignKeyField(H_User, backref='h_who_write_user')

class L_Asset_WhoProfile(BaseModel):
    asset = ForeignKeyField(H_Asset)
    who_profile = ForeignKeyField(H_WhoProfile)
    
    version = IntegerField()
    timestamp = DateTimeField()
    user = ForeignKeyField(H_User, backref='l_awho_user')

class S_WhoProfile_schema(BaseModel):
    who_profile = ForeignKeyField(H_WhoProfile)
    schema = JSONField()
    
    version = IntegerField()
    timestamp = DateTimeField()
    user = ForeignKeyField(H_User, backref='l_awho_user')

class L_WhoProfileUser(BaseModel):
    who_profile = ForeignKeyField(H_WhoProfile)
    who_user = ForeignKeyField(H_User)
    
    version = IntegerField()
    timestamp = DateTimeField()
    user = ForeignKeyField(H_User, backref='l_uwho_user')

class H_HowProfile(BaseModel):
    how_profileId = IntegerField()
    
    version = IntegerField()
    timestamp = DateTimeField()
    user = ForeignKeyField(H_User, backref='h_how_user')
    
class S_HowProfile_schema(BaseModel):
    how_profile = ForeignKeyField(H_HowProfile)
    schema = JSONField()
    
    version = IntegerField()
    timestamp = DateTimeField()
    user = ForeignKeyField(H_User, backref='h_howschema_user')

class L_Asset_HowProfile(BaseModel):
    asset = ForeignKeyField(H_Asset)
    how_profile = ForeignKeyField(H_HowProfile)
    
    version = IntegerField()
    timestamp = DateTimeField()
    user = ForeignKeyField(H_User, backref='l_ahow_user')

class H_WhyProfile(BaseModel):
    why_profileId = IntegerField()
    
    version = IntegerField()
    timestamp = DateTimeField()
    user = ForeignKeyField(H_User, backref='h_why_user')
    
class S_WhyProfile_schema(BaseModel):
    why_profile = ForeignKeyField(H_WhyProfile)
    schema = JSONField()
    
    version = IntegerField()
    timestamp = DateTimeField()
    user = ForeignKeyField(H_User, backref='h_whyschema_user')

class L_Asset_WhyProfile(BaseModel):
    asset = ForeignKeyField(H_Asset)
    why_profile = ForeignKeyField(H_WhyProfile)
    
    version = IntegerField()
    timestamp = DateTimeField()
    user = ForeignKeyField(H_User, backref='l_awhy_user')

class H_WhatProfile(BaseModel):
    what_profileId = IntegerField()
    
    version = IntegerField()
    timestamp = DateTimeField()
    user = ForeignKeyField(H_User, backref='h_what_user')
    
class S_WhatProfile_schema(BaseModel):
    what_profile = ForeignKeyField(H_WhatProfile)
    schema = JSONField()
    
    version = IntegerField()
    timestamp = DateTimeField()
    user = ForeignKeyField(H_User, backref='h_whatschema_user')

class L_Asset_WhatProfile(BaseModel):
    asset = ForeignKeyField(H_Asset)
    what_profile = ForeignKeyField(H_WhatProfile)
    
    version = IntegerField()
    timestamp = DateTimeField()
    user = ForeignKeyField(H_User, backref='l_awhat_user')

class H_WhenProfile(BaseModel):
    when_profileId = IntegerField()
    
    version = IntegerField()
    timestamp = DateTimeField()
    user = ForeignKeyField(H_User, backref='h_when_user')

class S_WhenProfile_Attributes(BaseModel):
    asset_timestamp = DateTimeField()
    expiry_date = DateTimeField()
    start_date = DateTimeField()
    
    
    version = IntegerField()
    timestamp = DateTimeField()
    user = ForeignKeyField(H_User, backref='s_when_user')

class L_Asset_WhenProfile(BaseModel):
    asset = ForeignKeyField(H_Asset)
    when_profile = ForeignKeyField(H_WhenProfile)
    
    version = IntegerField()
    timestamp = DateTimeField()
    user = ForeignKeyField(H_User, backref='l_awhen_user')

class H_WhereProfile(BaseModel):
    where_profileId = IntegerField()
    access_path = TextField() #business key
    
    version = IntegerField()
    timestamp = DateTimeField()
    user = ForeignKeyField(H_User, backref='h_where_user')

class S_Configuration(BaseModel):
    schema = JSONField()
    where_profile = ForeignKeyField(H_WhereProfile)
    
    version = IntegerField()
    timestamp = DateTimeField()
    user = ForeignKeyField(H_User, backref='h_where_config')

class H_Source(BaseModel):
    sourceId = IntegerField()
    
    version = IntegerField()
    timestamp = DateTimeField()
    user = ForeignKeyField(H_User, backref='h_where_source')
    
class H_SourceType(BaseModel):
    source_typeId = IntegerField()
    
    version = IntegerField()
    timestamp = DateTimeField()
    user = ForeignKeyField(H_User, backref='h_where_sourcetype')

class S_SourceTypeAttributes(BaseModel):
    source_type = ForeignKeyField(H_SourceType)
    connector = TextField()
    serdetype = TextField()
    datamodel = TextField()
    
    version = IntegerField()
    timestamp = DateTimeField()
    user = ForeignKeyField(H_User, backref='s_where_sourcetype')

class L_Source2Type(BaseModel):
    source = ForeignKeyField(H_Source)
    source_type = ForeignKeyField(H_SourceType)
    
    version = IntegerField()
    timestamp = DateTimeField()
    user = ForeignKeyField(H_User, backref='l_awhere_user')

class L_Asset_WhereProfile(BaseModel):
    asset = ForeignKeyField(H_Asset)
    where_profile = ForeignKeyField(H_WhereProfile)
    
    version = IntegerField()
    timestamp = DateTimeField()
    user = ForeignKeyField(H_User, backref='h_asset_where')

class H_Action(BaseModel):
    actionId = IntegerField()
    
    version = IntegerField()
    timestamp = DateTimeField()
    user = ForeignKeyField(H_User, backref='h_asset_action')

class L_AssetsInActions(BaseModel):
    action = ForeignKeyField(H_Action)
    asset = ForeignKeyField(H_Asset)
    who_profile = ForeignKeyField(H_WhoProfile)
    why_profile = ForeignKeyField(H_WhyProfile)
    when_profile = ForeignKeyField(H_WhenProfile)
    how_profile = ForeignKeyField(H_HowProfile)

class H_RelationshipType(BaseModel):
    relationship_typeId = IntegerField()
    
    version = IntegerField()
    timestamp = DateTimeField()
    user = ForeignKeyField(H_User, backref='h_reltype_user')

class S_RelationshipTypeAttributes(BaseModel):
    name = TextField()
    description = TextField()
    
    version = IntegerField()
    timestamp = DateTimeField()
    user = ForeignKeyField(H_User, backref='s_reltype_user')
    
    

class H_Relationship(BaseModel):
    relationshipId = IntegerField()
    
    version = IntegerField()
    timestamp = DateTimeField()
    user = ForeignKeyField(H_User, backref='h_arel_user')

class S_Relationship_schema(BaseModel):
    relationship = ForeignKeyField(H_Relationship)
    schema = JSONField()

class L_Relationship_Type(BaseModel):
    relationship = ForeignKeyField(H_Relationship)
    relationship_type = ForeignKeyField(H_RelationshipType)
    
    version = IntegerField()
    timestamp = DateTimeField()
    user = ForeignKeyField(H_User, backref='l_reltype_user')

#the many-many relationship table for the 
#'relationship-to-asset' relationship
class L_Asset_Relationships(BaseModel):
    asset = ForeignKeyField(Asset, backref='rel_asset')
    relationship = ForeignKeyField(Relationship, backref='asset_rel')
    
    version = IntegerField()
    timestamp = DateTimeField()
    user = ForeignKeyField(H_User, backref='l_arel_user')
    
    
    
    
    