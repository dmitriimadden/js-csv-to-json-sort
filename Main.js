const fs = require('fs') 
const readline = require('readline');

///INPUT FILE 
const ProcessFile = './enrollment.csv'
//////////////////////////////////////


///OUTPUT FOLDER
const outputFolder ='./Output/'
//////

//Convert the file to JSON list
function convert(file) {
    return new Promise((resolve, reject) => {
        var LineNumber = 0

        const stream = fs.createReadStream(file);
        // Handle stream error (IE: file not found)
        stream.on('error', reject);

        const reader = readline.createInterface({
            input: stream
        });

        var database = [];

        reader.on('line', line => {
            LineNumber++;

            if (LineNumber > 1){
            let customer = {}
                var customer_line = (line.split(","));
                customer.UserId = customer_line[0]
                customer.FullName = customer_line[1]
                customer.Version = parseFloat(customer_line[2])
                customer.Insurance = customer_line[3]
            
            database.push(customer)
        }
        });
        reader.on('close', () => resolve(database));
    });
}

convert(ProcessFile)
    .then(database => {
        //Function to sort Names 
        function sortLastFirst(a, b) {
            var splitA = a.FullName.split(" ");
            var splitB = b.FullName.split(" ");
            var lastA = splitA[splitA.length - 1];
            var lastB = splitB[splitB.length - 1];
        
            if (lastA < lastB) return -1;
            if (lastA > lastB) return 1;
            return 0;
        }
        //function to split Companies and sort them by name
        function sortCompanies(database){
            database = database.sort((a, b) => (a.Insurance > b.Insurance) ? 1 : -1)
            prev_Insurance=database[0].Insurance
            var companies = []
            var company = []

            for (i=0; i<database.length; i++){
                if (database[i].Insurance == prev_Insurance)
                    company.push(database[i])
                else {
                    companies.push(company)
                    company = []
                    company.push(database[i])
                    prev_Insurance=database[i].Insurance
                }
            }
            return companies
        }
        //Remove old versions of the dublicate UIDs
        function eliminateDuplicates(database){
            database = database.sort((a, b) => (a.UserId > b.UserId) ? 1 : -1)
            var prev_customer = database[0]
            var databaseFiltered = []


            databaseFiltered.push(prev_customer)

            for (i=1; i<database.length-1; i++){
                if (database[i].UserId == prev_customer.UserId){
                        if (database[i].Version > prev_customer.Version){
                            databaseFiltered.pop()
                            databaseFiltered.push(database[i])
                            prev_customer = database[i]
                        }
            }
                else{
                prev_customer = database[i]
                databaseFiltered.push(prev_customer)
                    }
            }


            return (databaseFiltered)

        }
        //Convert JSON to CSV and EXPORT
        function ConvertToCSV(objArray) {
            var array = typeof objArray != 'object' ? JSON.parse(objArray) : objArray;
            var str = '';

            for (var i = 0; i < array.length; i++) {
                var line = '';
                for (var index in array[i]) {
                    if (line != '') line += ','

                    line += array[i][index];
                }

                str += line + '\r\n';
            }

            return str;
        }
        

        let companies = sortCompanies(database)
        companies.forEach(function (company) {
            let company_temp = eliminateDuplicates(company)
            let company_temp2 =  company_temp.sort(sortLastFirst)
            let header = {}
            header.UserId = "User Id"
            header.FullName = "First and Last Name"
            header.Version = "Version"
            header.Insurance = "Insurance Company"
        
            company_temp2.unshift(header)
            let jsonObject = JSON.stringify(company_temp2);
            let csv = ConvertToCSV(jsonObject)
            fs.writeFile(outputFolder + company[0].Insurance + '.csv', csv, (err) => {
              if (err) throw err;
              console.log(company[0].Insurance +'.csv saved.');
            });
        });
        console.log('CSV files successfully processed');

    })
    .catch(err => console.error(err));





