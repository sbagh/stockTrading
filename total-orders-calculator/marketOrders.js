const fs = require("fs");
const ndjson = require("ndjson");

// Correct: Directly create a read stream from the file path
const readStream = fs
   .createReadStream(
      "../stock-data/smci/XNAS-20240217-DR3J9CCF3H/xnas-itch-20240111.mbo.json"
   )
   .pipe(ndjson.parse());

// Create a write stream for the filtered NDJSON file
const writeStream = fs.createWriteStream("filteredOrders.json");

// Initialize counters for each side
const sums = {
   A: 0,
   B: 0,
   other: 0,
};

readStream.on("data", function (obj) {
   // Filter out records with action "T" and order_id "0"
   if (obj.action === "T" && obj.order_id === "0") {
      writeStream.write(JSON.stringify(obj) + "\n");
      // Sum the sizes based on the side of each order
      if (obj.side === "A") {
         sums.A += obj.size;
      } else if (obj.side === "B") {
         sums.B += obj.size;
      } else {
         sums.other += obj.size;
      }
   }

});

readStream.on("end", function () {
   writeStream.end(); // Ensure to close the write stream
   console.log(
      "Finished processing the file. Filtered orders have been written to filteredOrders.ndjson."
   );

   // Log the sums for each side
   console.log(`Sum of sizes for side A: ${sums.A}`);
   console.log(`Sum of sizes for side B: ${sums.B}`);
   console.log(`Sum of sizes for other sides: ${sums.other}`);
});

readStream.on("error", function (error) {
   console.error("Error while reading the file:", error);
});

writeStream.on("error", function (error) {
   console.error("Error while writing to the file:", error);
});
