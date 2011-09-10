using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Dynamic;

namespace TmMq
{
    public class DynamicDictionary : DynamicObject, IDictionary<string, object>
    {
        protected ConcurrentDictionary<string, object> Fields { get; private set; }

        public bool ThrowOnMissing { get; set; }

        public DynamicDictionary()
        {
            Fields = new ConcurrentDictionary<string, object>( StringComparer.CurrentCultureIgnoreCase );
        }

        public DynamicDictionary( IEnumerable<KeyValuePair<string, object>> values )
        {
            Fields = new ConcurrentDictionary<string, object>( values, StringComparer.CurrentCultureIgnoreCase );
        }

        public override bool TryGetMember( GetMemberBinder binder, out object result )
        {
            return Fields.TryGetValue( binder.Name, out result );
        }

        public override bool TrySetMember( SetMemberBinder binder, object value )
        {
            Fields[binder.Name] = value;
            return true;
        }

        public bool HasField( string name )
        {
            return Fields.ContainsKey( name );
        }

        public object this[string fieldName]
        {
            get
            {
                if( !ThrowOnMissing && !Fields.ContainsKey( fieldName ) )
                {
                    return null;
                }

                return Fields[fieldName];
            }
            set { Fields[fieldName] = value; }
        }

        public object this[int columnNumber]
        {
            get
            {
                if( !ThrowOnMissing && !Fields.ContainsKey( columnNumber.ToString() ) )
                {
                    return null;
                }

                return Fields[columnNumber.ToString()];
            }
            set { Fields[columnNumber.ToString()] = value; }
        }

        public ICollection<string> Keys
        {
            get { return Fields.Keys; }
        }

        public ICollection<object> Values
        {
            get { return Fields.Values; }
        }

        #region IDictionary<string,object> Members

        public void Add( string key, object value )
        {
            Fields[key] = value;
        }

        public bool ContainsKey( string key )
        {
            return Fields.ContainsKey( key );
        }

        public bool Remove( string key )
        {
            object value;
            return Fields.TryRemove( key, out value );
        }

        public bool TryGetValue( string key, out object value )
        {
            return Fields.TryGetValue( key, out value );
        }

        #endregion

        #region ICollection<KeyValuePair<string,object>> Members

        public void Add( KeyValuePair<string, object> item )
        {
            Fields[item.Key] = item.Value;
        }

        public void Clear()
        {
            Fields.Clear();
        }

        public bool Contains( KeyValuePair<string, object> item )
        {
            return Fields.ContainsKey( item.Key );
        }

        public void CopyTo( KeyValuePair<string, object>[] array, int arrayIndex )
        {
            throw new NotImplementedException();
        }

        public int Count
        {
            get { return Fields.Count; }
        }

        public bool IsReadOnly
        {
            get { return false; }
        }

        public bool Remove( KeyValuePair<string, object> item )
        {
            return Remove( item.Key );
        }

        #endregion

        #region IEnumerable<KeyValuePair<string,object>> Members

        public IEnumerator<KeyValuePair<string, object>> GetEnumerator()
        {
            return Fields.GetEnumerator();
        }

        #endregion

        #region IEnumerable Members

        System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator()
        {
            return Fields.GetEnumerator();
        }

        #endregion
    }

}
