using System;                                                                                             
using MongoDB.Bson;
using MongoDB.Bson.IO;
using MongoDB.Bson.Serialization;
using MongoDB.Bson.Serialization.Conventions;
using MongoDB.Bson.Serialization.Serializers;

namespace TmMq
{
    public class DynamicDictionarySerializer : IBsonSerializer
    {
        public object Deserialize( BsonDeserializationContext context, BsonDeserializationArgs args )
        {
            var bsonReader = context.Reader;
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
                        object value = valueSerializer.Deserialize( context );

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
                    string message = string.Format( "Can't deserialize a {0} from BsonType {1}.", context.Reader.CurrentBsonType, bsonType );
                    throw new BsonException( message );
                }
            }

            return result;
        }

        public void Serialize( BsonSerializationContext context, BsonSerializationArgs args, object value )
        {
            var dictionary = (DynamicDictionary)value;

            var bsonWriter = context.Writer;
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

        public Type ValueType
        {
            get { return typeof( DynamicDictionary ); }
        }
    }
}