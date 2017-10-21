using Microsoft.Hadoop.MapReduce;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace EvenOrOddCount
{
    public class MySimpleReducer : ReducerCombinerBase
    {
        public override void Reduce(string key, IEnumerable<string> values, ReducerCombinerContext context)
        {
            int myCount = 0;
            int mySum = 0;

            foreach (string value in values)
            {
                if (int.TryParse(value, out int intValue))
                {
                    mySum += intValue;
                    myCount++;
                }
            }

            context.EmitKeyValue(key, "count:" + myCount + ";Sum:" + mySum + ".");
        }
    }
}
