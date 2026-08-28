using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Genesys___analytics_conversations_details_query
{
    public class UsersResponse
    {
        public List<UserResult> Results { get; set; }
    }

    public class UserResult
    {
        public string Id { get; set; }
        public string Name { get; set; }
        public string Email { get; set; }
        public string State { get; set; }

    }
}
