using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Genesys___analytics_conversations_details_query
{
    public class UsersRoot
    {
        public List<UserEntity> Entities { get; set; }
    }

    public class UserEntity
    {
        public string Id { get; set; }
        public string Name { get; set; }
        public string Email { get; set; }
        public string Username { get; set; }
        public string State { get; set; }

        public Division Division { get; set; }
        public Chat Chat { get; set; }
    }

    public class Division
    {
        public string Id { get; set; }
        public string Name { get; set; }
    }

    public class Chat
    {
        public string JabberId { get; set; }
    }
}
