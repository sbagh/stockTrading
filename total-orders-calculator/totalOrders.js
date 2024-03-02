const fs = require("fs");
const path = require("path");
const readline = require("readline");

async function processFile(filePath) {
   let dailyTotals = {
      totalAsks: 0,
      totalBids: 0,
      totalTrades: 0,
      totalCancel: 0,
      totalFilled: 0,
   };

   let hourlyTotals = {};

   const fileStream = fs.createReadStream(filePath);
   const rl = readline.createInterface({
      input: fileStream,
      crlfDelay: Infinity,
   });

   for await (const line of rl) {
      const order = JSON.parse(line);
      let size = parseInt(order.size, 10);
      let hour = extractHour(order.hd.ts_event);

      if (!hourlyTotals[hour]) {
         hourlyTotals[hour] = {
            totalAsks: 0,
            totalBids: 0,
            totalTrades: 0,
            totalCancel: 0,
            totalFilled: 0,
         };
      }

      switch (order.action) {
         case "A":
            if (order.side === "A") {
               dailyTotals.totalAsks += size;
               hourlyTotals[hour].totalAsks += size;
            } else if (order.side === "B") {
               dailyTotals.totalBids += size;
               hourlyTotals[hour].totalBids += size;
            }
            break;
         case "T":
            dailyTotals.totalTrades += size;
            hourlyTotals[hour].totalTrades += size;
            break;
         case "F":
            dailyTotals.totalFilled += size;
            hourlyTotals[hour].totalFilled += size;
            break;
         case "C":
            dailyTotals.totalCancel += size;
            hourlyTotals[hour].totalCancel += size;
            break;
      }
   }

   return { dailyTotals, hourlyTotals };
}

function extractHour(timestamp) {
   let date = new Date(timestamp);
   return date.getHours();
}

async function calculateSummary(directoryPath) {
   const dailyOutputStream = fs.createWriteStream(
      "./outputs/dailyTotals.ndjson"
   );
   const hourlyOutputStream = fs.createWriteStream(
      "./outputs/hourlyTotals.ndjson"
   );

   const files = fs
      .readdirSync(directoryPath)
      .filter((file) => file.match(/^xnas-itch-\d{8}\.mbo\.json$/));

   for (const file of files) {
      const filePath = path.join(directoryPath, file);
      console.log(`Processing file: ${filePath}`);
      const { dailyTotals, hourlyTotals } = await processFile(filePath);

      // Extract the date from the file name
      const dateMatch = file.match(/^xnas-itch-(\d{8})\.mbo\.json$/);
      const date = dateMatch
         ? `${dateMatch[1].substring(0, 4)}/${dateMatch[1].substring(
              4,
              6
           )}/${dateMatch[1].substring(6, 8)}`
         : "unknown";

      // Include the date in the dailyTotals
      const dailyTotalsWithDate = { date, ...dailyTotals };
      dailyOutputStream.write(JSON.stringify(dailyTotalsWithDate) + "\n");

      // Write hourlyTotals to NDJSON (assuming you want to keep hourly totals as they are)
      Object.keys(hourlyTotals).forEach((hour) => {
         const data = {
            date, // Optionally include date in hourly totals if desired
            hour,
            ...hourlyTotals[hour],
         };
         hourlyOutputStream.write(JSON.stringify(data) + "\n");
      });
   }

   dailyOutputStream.end();
   hourlyOutputStream.end();

   console.log("Finished processing and writing to NDJSON files.");
}

const directoryPath = "../stock-data/smci/XNAS-20240217-DR3J9CCF3H";
calculateSummary(directoryPath);
