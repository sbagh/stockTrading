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

let hourlyTotals = {};

rl.on("line", (line) => {
   const order = JSON.parse(line);
   let size = parseInt(order.size, 10);
   let hour = getHourFromTimestamp(order.hd.ts_event); // Accessing ts_event within hd

   if (!hourlyTotals[hour]) {
      hourlyTotals[hour] = {
         A_sideA: 0,
         A_sideB: 0,
         total_T: 0,
         total_C: 0,
         total_F: 0,
      };
   }

   switch (order.action) {
      case "A":
         if (order.side === "A") {
            hourlyTotals[hour].A_sideA += size;
         } else if (order.side === "B") {
            hourlyTotals[hour].A_sideB += size;
         }
         break;
      case "T":
         hourlyTotals[hour].total_T += size;
         break;
      case "C":
         hourlyTotals[hour].total_C += size;
         break;
      case "F":
         hourlyTotals[hour].total_F += size;
         break;
   }
});

rl.on("close", () => {
   Object.keys(hourlyTotals).forEach((hour) => {
      const data = {
         hour: hour,
         ...hourlyTotals[hour],
      };
      outputStream.write(JSON.stringify(data) + "\n");
   });
   outputStream.end();
   console.log("Finished processing and writing to NDJSON file.");
});

function formatDate(date) {
   const twoDigits = (num) => String(num).padStart(2, "0");

   return (
      twoDigits(date.getDate()) +
      "/" +
      twoDigits(date.getMonth() + 1) +
      "/" +
      date.getFullYear().toString().slice(-2) +
      " - " +
      twoDigits(date.getHours()) +
      ":" +
      twoDigits(date.getMinutes()) +
      ":" +
      twoDigits(date.getSeconds())
   );
}

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
