using System;
using MongoDB.Bson;
using MongoDB.Bson.IO;
using MongoDB.Bson.Serialization;
using MongoDB.Bson.Serialization.Conventions;
using MongoDB.Bson.Serialization.Serializers;

namespace TmMq
{
    public class DynamicDictionarySerializer : BsonBaseSerializer
    {
        public override object Deserialize( BsonReader bsonReader, Type nominalType, IBsonSerializationOptions options )
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

                    IDiscriminatorConvention valueDiscriminatorConvention = BsonSerializer.LookupDiscriminatorConvention( typeof( object ) );

                    while( bsonReader.ReadBsonType() != BsonType.EndOfDocument )
                    {
                        string key = bsonReader.ReadName();
                        Type valueType = valueDiscriminatorConvention.GetActualType( bsonReader, typeof( object ) );
                        IBsonSerializer valueSerializer = BsonSerializer.LookupSerializer( valueType );
                        object value = valueSerializer.Deserialize( bsonReader, typeof( object ), valueType, null );

                        if( key != "_t" )
                        {
                            dictionary.Add( key.Replace( '\x03', '.' ), value );
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

        public override object Deserialize( BsonReader bsonReader, Type nominalType, Type actualType, IBsonSerializationOptions options )
        {
            return this.Deserialize( bsonReader, nominalType, options );
        }

        public override void Serialize( BsonWriter bsonWriter, Type nominalType, object value, IBsonSerializationOptions options )
        {
            var dictionary = (DynamicDictionary)value;

            bsonWriter.WriteStartDocument();

            bsonWriter.WriteString( "_t", "DynamicDictionary" );

            if( dictionary != null )
            {
                foreach( var entry in dictionary )
                {
                    bsonWriter.WriteName( entry.Key.Replace( '.', '\x03' ) );
                    BsonSerializer.Serialize( bsonWriter, typeof( object ), entry.Value );
                }
            }

            bsonWriter.WriteEndDocument();
        }
    }
}