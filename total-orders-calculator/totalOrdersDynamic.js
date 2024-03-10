const fs = require("fs");
const path = require("path");
const readline = require("readline");

// calculate all the total orders (ask, bid, cancel, trade, fill)
// for a directory of files and write the results to a json
const calculateSummary = async (directoryPath, timeInterval) => {
   // Create a writable stream for the output
   const outputStream = fs.createWriteStream(
      `./outputs/totals_${timeInterval}.json`
   );

   // Get all files in the directory that match the expected file name format
   const files = fs
      .readdirSync(directoryPath)
      .filter((file) => file.match(/^xnas-itch-\d{8}\.mbo\.json$/));

   // Process each file
   for (const file of files) {
      const filePath = path.join(directoryPath, file);
      console.log(`Processing file: ${filePath}`);

      // Process the file
      // write the time bin, totalAsks, totalBids, totalTrades, totalCancel, and totalFilled to the output stream
      const totals = await processFile(filePath, timeInterval);
      for (const [bin, data] of Object.entries(totals)) {
         const outputData = {
            bin,
            ...data,
         };
         outputStream.write(JSON.stringify(outputData) + "\n");
      }
   }

   outputStream.end();
   console.log("Finished processing files");
};

// Process a single file with a dynamic time interval for aggregation
const processFile = async (filePath, timeInterval) => {
   let totals = {};

   const fileStream = fs.createReadStream(filePath);
   const rl = readline.createInterface({
      input: fileStream,
      crlfDelay: Infinity,
   });

   // Process each line in the file (order by order)
   for await (const line of rl) {
      const order = JSON.parse(line);
      const size = parseInt(order.size, 10);

      // Calculate the time bin for this order
      const bin = calculateTimeBin(order.hd.ts_event, timeInterval);

      if (!totals[bin]) {
         totals[bin] = {
            totalAsks: 0,
            totalBids: 0,
            totalTrades: 0,
            totalCancel: 0,
            totalFilled: 0,
            marketOrders: {
               A: 0,
               B: 0,
               other: 0,
            },
         };
      }

      switch (order.action) {
         case "A":
            if (order.side === "A") {
               totals[bin].totalAsks += size;
            } else if (order.side === "B") {
               totals[bin].totalBids += size;
            }
            break;
         case "T":
            totals[bin].totalTrades += size;
            if (order.order_id === "0") {
               // Assumes market orders have order_id "0"
               if (order.side === "A") {
                  totals[bin].marketOrders.A += size;
               } else if (order.side === "B") {
                  totals[bin].marketOrders.B += size;
               } else {
                  totals[bin].marketOrders.other += size;
               }
            }
            break;
         case "F":
            totals[bin].totalFilled += size;
            break;
         case "C":
            totals[bin].totalCancel += size;
            break;
      }
   }

   return totals;
};

// Helper function to format date and time
const formatDateTime = (timestamp) => {
   const date = new Date(timestamp);
   return date.toISOString().replace(/T/, " ").replace(/\..+/, ""); // Formats the date to a more readable form without milliseconds
};

// Modified function to calculate the time bin and return it in date and time format
const calculateTimeBin = (timestamp, timeInterval) => {
   const date = new Date(timestamp);
   const epochSeconds = date.getTime() / 1000;
   const binStartSeconds =
      Math.floor(epochSeconds / timeInterval) * timeInterval;
   const binStartDate = new Date(binStartSeconds * 1000);
   return formatDateTime(binStartDate);
};

// Run the function with the specified directory and time interval
const directoryPath = "../stock-data/smci/XNAS-20240217-DR3J9CCF3H";
const timeInterval = 3600; // seconds
calculateSummary(directoryPath, timeInterval);
