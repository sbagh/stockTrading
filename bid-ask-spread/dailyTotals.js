const fs = require("fs");
const path = require("path");
const readline = require("readline");

const directoryPath = "../stock-data/smci/XNAS-20240217-DR3J9CCF3H";
const filesPattern = /^xnas-itch-(\d{8})\.mbo\.json$/;

// store overall totals across all files
let overallTotals = {
   A_sideA: 0,
   A_sideB: 0,
   total_T: 0,
   total_C: 0,
   total_F: 0,
};

const processFile = (filePath) => {
   return new Promise((resolve, reject) => {
      // Create a stream to read the file
      const fileStream = fs.createReadStream(filePath);
      const rl = readline.createInterface({
         input: fileStream,
         crlfDelay: Infinity,
      });

      // store daily totals for the current file
      let dailyTotals = {
         A_sideA: 0,
         A_sideB: 0,
         total_T: 0,
         total_C: 0,
         total_F: 0,
      };

      rl.on("line", (line) => {
         const order = JSON.parse(line);
         let size = parseInt(order.size, 10);
         // Update daily and overall totals based on the action and side
         switch (order.action) {
            case "A":
               if (order.side === "A") {
                  dailyTotals.A_sideA += size;
                  overallTotals.A_sideA += size;
               } else if (order.side === "B") {
                  dailyTotals.A_sideB += size;
                  overallTotals.A_sideB += size;
               }
               break;
            case "T":
               dailyTotals.total_T += size;
               overallTotals.total_T += size;
               break;
            case "C":
               dailyTotals.total_C += size;
               overallTotals.total_C += size;
               break;
            case "F":
               dailyTotals.total_F += size;
               overallTotals.total_F += size;
               break;
         }
      });

      rl.on("close", () => {
         resolve(dailyTotals);
      });

      rl.on("error", reject);
   });
};

const processAllFiles = async () => {
   const files = fs.readdirSync(directoryPath);
   // Filter files that match the pattern
   const jsonFiles = files.filter((file) => filesPattern.test(file));
   // store daily totals for all files
   let dailyTotalsArray = [];

   // Iterate through each file and process it
   for (const file of jsonFiles) {
      const filePath = path.join(directoryPath, file);
      const dailyTotals = await processFile(filePath);
      const dateMatch = file.match(filesPattern);

      // Format the date as "YYYY/MM/DD"
      const date = dateMatch
         ? `${dateMatch[1].substring(0, 4)}/${dateMatch[1].substring(
              4,
              6
           )}/${dateMatch[1].substring(6, 8)}`
         : "unknown";

      dailyTotalsArray.push({ date, ...dailyTotals });
   }

   fs.writeFileSync(
      "./outputs/dailyTotals.json",
      JSON.stringify(dailyTotalsArray, null, 2)
   );

   fs.writeFileSync(
      "./outputs/overallTotals.json",
      JSON.stringify({ "Overall Totals": overallTotals }, null, 2)
   );
   console.log(
      "Finished processing all files. Overall Totals: ",
      overallTotals
   );
};

processAllFiles();
processAllFiles();
