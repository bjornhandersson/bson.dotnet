namespace bson.Dispatcher.Hash
{
    public class FNV1a : IHashGenerator
    {
        const int PRIME = 16777619;

        public uint Hash(byte[] key)
        {
            uint hash = 2166136261;
            for (int i = 0; i < key.Length; i++)
            {
                hash = (hash ^ key[i]) * PRIME;
            }

            return hash;
        }

        public uint Hash(string key)
        {
            return Hash(System.Text.Encoding.UTF8.GetBytes(key));
        }
    }
}
