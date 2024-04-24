using Microsoft.VisualBasic.FileIO;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ParserGeology
{
    public delegate void ResultCallbackDelegate(string[] Results);
    public class LineParser
    {
        //Creating two private variables to hold the Number and ResultCallback instance
        private string gorparse;
        private ResultCallbackDelegate _resultCallbackDelegate;
        //Initializing the private variables through constructor
        //So while creating the instance you need to pass the value for Number and callback delegate
        public LineParser(string Number, ResultCallbackDelegate resultCallbackDelagate)
        {
            gorparse = Number;
            _resultCallbackDelegate = resultCallbackDelagate;
        }
       
        public void ParseStr()
        {
            string Result = "0";
            string[] goparses;

            StringReader reader = new StringReader(gorparse);
            using (TextFieldParser parser = new TextFieldParser(reader))
            {
                parser.TextFieldType = FieldType.Delimited;
                parser.SetDelimiters(";");
                goparses = parser.ReadFields();

            }
            //Before the end of the thread function call the callback method
            if (_resultCallbackDelegate != null)
            {
                _resultCallbackDelegate(goparses);
            }

        }
    }
}
