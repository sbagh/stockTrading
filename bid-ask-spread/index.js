const fs = require("fs");
const readline = require("readline");

const fileStream = fs.createReadStream(
   "./smci/XNAS-20240217-DR3J9CCF3H/xnas-itch-20231218.mbo.json"
);
const outputStream = fs.createWriteStream("./output1.json");

const rl = readline.createInterface({
   input: fileStream,
   crlfDelay: Infinity,
});

const bestBids = [];
const bestAsks = [];
const MAX_LENGTH = 10;

outputStream.write("[");
let firstLine = true;

// Update or insert an element in sorted order, depending if the price already exists
function updateOrInsert(array, newOrder, compareFn) {
   let found = false;
   // Check if the price already exists
   for (let i = 0; i < array.length; i++) {
      if (array[i].price === newOrder.price) {
         // if price exists, update the size
         array[i].size += newOrder.size;
         found = true;
         break;
      }
   }
   // if price doesn't exist, insert it in sorted order
   if (!found) {
      insertSorted(array, newOrder, compareFn);
   }
   if (array.length > MAX_LENGTH) {
      array.pop();
   }
}

// Insert element into array in a defined order
function insertSorted(array, element, compareFn) {
   let insertionIndex = array.length;
   for (let i = 0; i < array.length; i++) {
      if (compareFn(element, array[i]) < 0) {
         insertionIndex = i;
         break;
      }
   }
   array.splice(insertionIndex, 0, element);
}

// Function to remove or update the size of an order
function handleCancellation(array, canceledOrder) {
   for (let i = 0; i < array.length; i++) {
      if (array[i].order_id === canceledOrder.order_id) {
         array[i].size -= canceledOrder.size;
         if (array[i].size <= 0) {
            array.splice(i, 1); // Remove the order if size is zero or negative
         }
         break;
      }
   }
}

rl.on("line", (line) => {
   const order = JSON.parse(line);

   if (order.action === "A") {
      let price = parseInt(order.price, 10) / 1e9;
      let size = parseInt(order.size, 10);

      if (order.side === "A") {
         updateOrInsert(
            bestAsks,
            { price, size, order_id: order.order_id },
            (a, b) => a.price - b.price
         );
      } else if (order.side === "B") {
         updateOrInsert(
            bestBids,
            { price, size, order_id: order.order_id },
            (a, b) => b.price - a.price
         );
      }

      if (order.side === "A" || order.side === "B") {
         let result = {
            ts_event: order.ts_event,
            action: order.action,
            side: order.side,
            price: price,
            size: size,
            order_id: order.order_id,
            best_bids: bestBids,
            best_asks: bestAsks,
            spread:
               bestAsks.length > 0 && bestBids.length > 0
                  ? bestAsks[0].price - bestBids[0].price
                  : null,
         };

         if (!firstLine) outputStream.write(",");
         outputStream.write("\n" + JSON.stringify(result));
         firstLine = false;
      }
   }
});

rl.on("close", () => {
   outputStream.write("\n]");
   outputStream.end();
   console.log("Finished processing and writing to the file.");
});
