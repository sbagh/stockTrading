const fs = require("fs");
const readline = require("readline");

const fileStream = fs.createReadStream(
   "../stock-data/smci/XNAS-20240217-DR3J9CCF3H/xnas-itch-20240202.mbo.json"
);
const outputStream = fs.createWriteStream("./outputs/hourlyTotals.json");

const rl = readline.createInterface({
   input: fileStream,
   crlfDelay: Infinity,
});

// store totals for each hour
let hourlyTotals = {};
// store totals for the entire file
let overallTotals = {
   A_sideA: 0,
   A_sideB: 0,
   total_T: 0,
   total_C: 0,
   total_F: 0,
};

rl.on("line", (line) => {
   const order = JSON.parse(line);

   let size = parseInt(order.size, 10); // Convert the size field to an integer
   let hour = getHourFromTimestamp(order.hd.ts_event); // get the hour from the timestamp

   // initialize the hourlyTotals object if it doesn't exist
   if (!hourlyTotals[hour]) {
      hourlyTotals[hour] = {
         A_sideA: 0,
         A_sideB: 0,
         total_T: 0,
         total_C: 0,
         total_F: 0,
      };
   }

   // Process the order based on its action and update totals
   switch (order.action) {
      case "A":
         if (order.side === "A") {
            hourlyTotals[hour].A_sideA += size;
            overallTotals.A_sideA += size;
         } else if (order.side === "B") {
            hourlyTotals[hour].A_sideB += size;
            overallTotals.A_sideB += size;
         }
         break;
      case "T":
         hourlyTotals[hour].total_T += size;
         overallTotals.total_T += size;
         break;
      case "C":
         hourlyTotals[hour].total_C += size;
         overallTotals.total_C += size;
         break;
      case "F":
         hourlyTotals[hour].total_F += size;
         overallTotals.total_F += size;
         break;
   }
});

rl.on("close", () => {
   // Write each hour's totals to the output file
   Object.keys(hourlyTotals).forEach((hour) => {
      const data = {
         hour: hour,
         ...hourlyTotals[hour],
      };
      outputStream.write(JSON.stringify(data) + "\n");
   });

   // Writing overall Totals to the file
   outputStream.write("Overall Totals:\n");
   outputStream.write(JSON.stringify(overallTotals) + "\n");

   outputStream.end();
   console.log("Finished processing and writing to NDJSON file.");
});

// Helper function to extract the hour from a timestamp
function getHourFromTimestamp(ts_event) {
   let date = new Date(ts_event); // Parse ISO 8601 format directly
   return (
      date.getFullYear().toString().slice(-2) +
      "/" +
      String(date.getMonth() + 1).padStart(2, "0") +
      "/" +
      String(date.getDate()).padStart(2, "0") +
      " - " +
      String(date.getHours()).padStart(2, "0") +
      ":00:00"
   );
}
