namespace bson.Dispatcher.Hash
{
    public interface IHashGenerator
    {
        public uint Hash(byte[] key);

        public uint Hash(string key);
    }
}
