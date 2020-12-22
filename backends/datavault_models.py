from peewee import *
from playhouse.sqlite_ext import *

## Schema Models

database = SqliteDatabase(None)

class BaseModel(Model):
    class Meta:
        database = database
        
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
    #...but there are cases where another user inserts the name of some user
    user = DeferredForeignKey('H_User', null=True)

class S_User_schema(BaseModel):
    user = ForeignKeyField(H_User,  null=True, backref='h_schema_user')
    schema = JSONField()
    
    version = IntegerField()
    timestamp = DateTimeField()
    write_user = ForeignKeyField(H_User, null=True)

#name and description shouldn't be part of a hub table
#TODO: come back and fix above
class H_UserType(BaseModel):
    #user_typeId = IntegerField()
    
    version = IntegerField()
    timestamp = DateTimeField()
    user = ForeignKeyField(H_User, null=True)

class S_UserTypeAttributes(BaseModel):
    name = TextField()
    description = TextField()
    
    version = IntegerField()
    timestamp = DateTimeField()
    user = ForeignKeyField(H_User, null=True)

class L_UserTypeLink(BaseModel):
    user = ForeignKeyField(H_User,  null=True, backref='l_user')
    user_type = ForeignKeyField(H_UserType, backref='l_type')
    
    version = IntegerField()
    timestamp = DateTimeField()
    write_user = ForeignKeyField(H_User, null=True)

class H_Asset(BaseModel):
    #NOT a primary key, but uniquely identifies
    #an asset. The primary key is the "item id", which identifies
    #a specific version of the asset
    #UPDATE: not needed, for reasons discussed above
    #assetId = IntegerField()
    name = TextField()
    version = IntegerField()
    timestamp = DateTimeField()
    user = ForeignKeyField(H_User, null=True, backref='h_asset_user')

class H_AssetType(BaseModel):
    #asset_typeId = IntegerField()
    
    version = IntegerField()
    timestamp = DateTimeField()
    user = ForeignKeyField(H_User, null=True, backref='h_atype_user')
    
class S_AssetTypeAttributes(BaseModel):
    name = TextField()
    description = TextField()
    
    version = IntegerField()
    timestamp = DateTimeField()
    user = ForeignKeyField(H_User, null=True, backref='s_atype_user')
    

class L_AssetTypeLink(BaseModel):
    asset = ForeignKeyField(H_Asset, backref='l_type_asset')
    asset_type = ForeignKeyField(H_AssetType, backref='l_asset_type')
    
    version = IntegerField()
    timestamp = DateTimeField()
    user = ForeignKeyField(H_User, null=True, backref='l_atype_user')


class H_WhoProfile(BaseModel):
     #NOT a primary key, but uniquely identifies
    #some version of a who-profile for an asset. The primary key is the "item id", which identifies
    #a specific version of the asset
    #who_profileId = IntegerField()
    
    version = IntegerField()
    timestamp = DateTimeField()
    user = ForeignKeyField(H_User, null=True, backref='h_who_write_user')

class L_Asset_WhoProfile(BaseModel):
    asset = ForeignKeyField(H_Asset)
    who_profile = ForeignKeyField(H_WhoProfile)
    
    version = IntegerField()
    timestamp = DateTimeField()
    user = ForeignKeyField(H_User, null=True, backref='l_awho_user')

class S_WhoProfile_schema(BaseModel):
    who_profile = ForeignKeyField(H_WhoProfile)
    schema = JSONField()
    user = ForeignKeyField(H_User, null=True)
    version = IntegerField()
    timestamp = DateTimeField()
    write_user = ForeignKeyField(H_User, null=True)

class L_WhoProfileUser(BaseModel):
    who_profile = ForeignKeyField(H_WhoProfile)
    who_user = ForeignKeyField(H_User)
    
    version = IntegerField()
    timestamp = DateTimeField()
    user = ForeignKeyField(H_User, null=True, backref='l_uwho_user')

class H_HowProfile(BaseModel):
    #how_profileId = IntegerField()
    
    version = IntegerField()
    timestamp = DateTimeField()
    user = ForeignKeyField(H_User, null=True, backref='h_how_user')
    
class S_HowProfile_schema(BaseModel):
    how_profile = ForeignKeyField(H_HowProfile)
    schema = JSONField()
    
    version = IntegerField()
    timestamp = DateTimeField()
    user = ForeignKeyField(H_User, null=True, backref='h_howschema_user')

class L_Asset_HowProfile(BaseModel):
    asset = ForeignKeyField(H_Asset)
    how_profile = ForeignKeyField(H_HowProfile)
    
    version = IntegerField()
    timestamp = DateTimeField()
    user = ForeignKeyField(H_User, null=True, backref='l_ahow_user')

class H_WhyProfile(BaseModel):
    #why_profileId = IntegerField()
    
    version = IntegerField()
    timestamp = DateTimeField()
    user = ForeignKeyField(H_User, null=True, backref='h_why_user')
    
class S_WhyProfile_schema(BaseModel):
    why_profile = ForeignKeyField(H_WhyProfile)
    schema = JSONField()
    
    version = IntegerField()
    timestamp = DateTimeField()
    user = ForeignKeyField(H_User, null=True, backref='h_whyschema_user')

class L_Asset_WhyProfile(BaseModel):
    asset = ForeignKeyField(H_Asset)
    why_profile = ForeignKeyField(H_WhyProfile)
    
    version = IntegerField()
    timestamp = DateTimeField()
    user = ForeignKeyField(H_User, null=True, backref='l_awhy_user')

class H_WhatProfile(BaseModel):
    #what_profileId = IntegerField()
    
    version = IntegerField()
    timestamp = DateTimeField()
    user = ForeignKeyField(H_User, null=True, backref='h_what_user')
    
class S_WhatProfile_schema(BaseModel):
    what_profile = ForeignKeyField(H_WhatProfile)
    schema = JSONField()
    
    version = IntegerField()
    timestamp = DateTimeField()
    user = ForeignKeyField(H_User, null=True, backref='h_whatschema_user')

class L_Asset_WhatProfile(BaseModel):
    asset = ForeignKeyField(H_Asset)
    what_profile = ForeignKeyField(H_WhatProfile)
    
    version = IntegerField()
    timestamp = DateTimeField()
    user = ForeignKeyField(H_User, null=True, backref='l_awhat_user')

class H_WhenProfile(BaseModel):
    #when_profileId = IntegerField()
    
    version = IntegerField()
    timestamp = DateTimeField()
    user = ForeignKeyField(H_User, null=True, backref='h_when_user')

class S_WhenProfile_Attributes(BaseModel):
    h_when = ForeignKeyField(H_WhenProfile)
    asset_timestamp = DateTimeField()
    expiry_date = DateTimeField()
    start_date = DateTimeField()
    
    version = IntegerField()
    timestamp = DateTimeField()
    user = ForeignKeyField(H_User, null=True, backref='s_when_user')

class L_Asset_WhenProfile(BaseModel):
    asset = ForeignKeyField(H_Asset)
    when_profile = ForeignKeyField(H_WhenProfile)
    
    version = IntegerField()
    timestamp = DateTimeField()
    user = ForeignKeyField(H_User, null=True, backref='l_awhen_user')

class H_WhereProfile(BaseModel):
    #where_profileId = IntegerField()
    access_path = TextField() #business key
    
    version = IntegerField()
    timestamp = DateTimeField()
    user = ForeignKeyField(H_User, null=True, backref='h_where_user')

class S_Configuration(BaseModel):
    schema = JSONField()
    where_profile = ForeignKeyField(H_WhereProfile)
    
    version = IntegerField()
    timestamp = DateTimeField()
    user = ForeignKeyField(H_User, null=True, backref='h_where_config')

class H_Source(BaseModel):
    #sourceId = IntegerField()
    name = TextField()
    version = IntegerField()
    timestamp = DateTimeField()
    user = ForeignKeyField(H_User, null=True, backref='h_where_source')
    
class S_Source_schema(BaseModel):
    schema = JSONField()
    version = IntegerField()
    timestamp = DateTimeField()
    user = ForeignKeyField(H_User, null=True)
    
class H_SourceType(BaseModel):
    #source_typeId = IntegerField()
    
    version = IntegerField()
    timestamp = DateTimeField()
    user = ForeignKeyField(H_User, null=True, backref='h_where_sourcetype')

class S_SourceTypeAttributes(BaseModel):
    source_type = ForeignKeyField(H_SourceType)
    connector = TextField()
    serdetype = TextField()
    datamodel = TextField()
    
    version = IntegerField()
    timestamp = DateTimeField()
    user = ForeignKeyField(H_User, null=True, backref='s_where_sourcetype')

class L_Source2Type(BaseModel):
    source = ForeignKeyField(H_Source)
    source_type = ForeignKeyField(H_SourceType)
    
    version = IntegerField()
    timestamp = DateTimeField()
    user = ForeignKeyField(H_User, null=True, backref='l_awhere_user')
    
class L_WhereProfile_Source(BaseModel):
    source = ForeignKeyField(H_Source)
    where_profile = ForeignKeyField(H_WhereProfile)
    
    version = IntegerField()
    timestamp = DateTimeField()
    user = ForeignKeyField(H_User, null=True)
    

class L_Asset_WhereProfile(BaseModel):
    asset = ForeignKeyField(H_Asset)
    where_profile = ForeignKeyField(H_WhereProfile)
    
    version = IntegerField()
    timestamp = DateTimeField()
    user = ForeignKeyField(H_User, null=True, backref='h_asset_where')

class H_Action(BaseModel):
    #actionId = IntegerField()
    
    version = IntegerField()
    timestamp = DateTimeField()
    user = ForeignKeyField(H_User, null=True, backref='h_asset_action')

class L_AssetsInActions(BaseModel):
    action = ForeignKeyField(H_Action)
    asset = ForeignKeyField(H_Asset)
    who_profile = ForeignKeyField(H_WhoProfile)
    why_profile = ForeignKeyField(H_WhyProfile)
    when_profile = ForeignKeyField(H_WhenProfile)
    how_profile = ForeignKeyField(H_HowProfile)
    
    version = IntegerField()
    timestamp = DateTimeField()
    user = ForeignKeyField(H_User, null=True)

class H_RelationshipType(BaseModel):
    #relationship_typeId = IntegerField()
    
    version = IntegerField()
    timestamp = DateTimeField()
    user = ForeignKeyField(H_User, null=True, backref='h_reltype_user')

class S_RelationshipTypeAttributes(BaseModel):
    name = TextField()
    description = TextField()
    
    version = IntegerField()
    timestamp = DateTimeField()
    user = ForeignKeyField(H_User, null=True, backref='s_reltype_user')
    
    

class H_Relationship(BaseModel):
    #relationshipId = IntegerField()
    
    version = IntegerField()
    timestamp = DateTimeField()
    user = ForeignKeyField(H_User, null=True, backref='h_arel_user')

class S_Relationship_schema(BaseModel):
    relationship = ForeignKeyField(H_Relationship)
    schema = JSONField()
    
    version = IntegerField()
    timestamp = DateTimeField()
    user = ForeignKeyField(H_User, null=True)

class L_Relationship_Type(BaseModel):
    relationship = ForeignKeyField(H_Relationship)
    relationship_type = ForeignKeyField(H_RelationshipType)
    
    version = IntegerField()
    timestamp = DateTimeField()
    user = ForeignKeyField(H_User, null=True, backref='l_reltype_user')

#the many-many relationship table for the 
#'relationship-to-asset' relationship
class L_Asset_Relationships(BaseModel):
    asset = ForeignKeyField(H_Asset, backref='rel_asset')
    relationship = ForeignKeyField(H_Relationship, backref='asset_rel')
    
    version = IntegerField()
    timestamp = DateTimeField()
    user = ForeignKeyField(H_User, null=True, backref='l_arel_user')
    
    
    
    
    