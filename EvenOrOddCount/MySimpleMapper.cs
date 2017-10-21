using Microsoft.Hadoop.MapReduce;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace EvenOrOddCount
{
    public class MySimpleMapper : MapperBase
    {
        public override void Map(string inputLine, MapperContext context)
        {
            if (int.TryParse(inputLine, out int value))
            {
                string key = value % 2 == 0 ? "even" : "odd";
                context.EmitKeyValue(key, value.ToString());
            }
        }
    }
}
