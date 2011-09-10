using System;
using MongoDB.Bson;
using MongoDB.Bson.IO;
using MongoDB.Bson.Serialization;
using MongoDB.Bson.Serialization.Conventions;

namespace TmMq
{
    public class DynamicDictionarySerializer : IBsonSerializer
    {
        public object Deserialize( BsonReader bsonReader, Type nominalType, IBsonSerializationOptions options )
        {
            BsonType bsonType = bsonReader.CurrentBsonType;

            object result;

            if( bsonType == BsonType.Null )
            {
                bsonReader.ReadNull();
                result = null;
            }
            else
            {
                if( bsonType == BsonType.Document )
                {
                    var dictionary = new DynamicDictionary();

                    bsonReader.ReadStartDocument();

                    IDiscriminatorConvention valueDiscriminatorConvention = BsonDefaultSerializer.LookupDiscriminatorConvention( typeof( object ) );

                    while( bsonReader.ReadBsonType() != BsonType.EndOfDocument )
                    {
                        string key = bsonReader.ReadName();
                        Type valueType = valueDiscriminatorConvention.GetActualType( bsonReader, typeof( object ) );
                        IBsonSerializer valueSerializer = BsonSerializer.LookupSerializer( valueType );
                        object value = valueSerializer.Deserialize( bsonReader, typeof( object ), valueType, null );

                        if( key != "_t" )
                        {
                            dictionary.Add( key, value );
                        }
                    }
                    bsonReader.ReadEndDocument();
                    result = dictionary;
                }
                else
                {
                    string message = string.Format( "Can't deserialize a {0} from BsonType {1}.", nominalType.FullName, bsonType );
                    throw new BsonException( message );
                }
            }

            return result;
        }

        public object Deserialize( BsonReader bsonReader, Type nominalType, Type actualType, IBsonSerializationOptions options )
        {
            return this.Deserialize( bsonReader, nominalType, options );
        }

        public bool GetDocumentId( object document, out object id, out Type idNominalType, out IIdGenerator idGenerator )
        {
            throw new NotImplementedException();
        }

        public void Serialize( BsonWriter bsonWriter, Type nominalType, object value, IBsonSerializationOptions options )
        {
            var dictionary = (DynamicDictionary)value;

            bsonWriter.WriteStartDocument();

            bsonWriter.WriteString( "_t", "DynamicDictionary" );

            if( dictionary != null )
            {
                foreach( var entry in dictionary )
                {
                    bsonWriter.WriteName( entry.Key );
                    BsonSerializer.Serialize( bsonWriter, typeof( object ), entry.Value );
                }
            }

            bsonWriter.WriteEndDocument();
        }

        public void SetDocumentId( object document, object id )
        {
            throw new NotImplementedException();
        }
    }
}