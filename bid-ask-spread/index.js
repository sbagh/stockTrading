const fs = require("fs");
const readline = require("readline");

const fileStream = fs.createReadStream(
   "../stock-data/smci/XNAS-20240217-DR3J9CCF3H/xnas-itch-20231218.mbo.json"
);
const outputStream = fs.createWriteStream("./outputs/jsOutput.json");

const rl = readline.createInterface({
   input: fileStream,
   crlfDelay: Infinity,
});

const bestBids = [];
const bestAsks = [];
const ARRAY_MAX_LENGTH = 10;

function updateOrInsert(array, newOrder, compareFn) {
   let found = false;
   for (let i = 0; i < array.length; i++) {
      if (array[i].price === newOrder.price) {
         array[i].size += newOrder.size;
         found = true;
         break;
      }
   }
   if (!found) {
      insertSorted(array, newOrder, compareFn);
   }
   if (array.length > ARRAY_MAX_LENGTH) {
      array.pop();
   }
}

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

function handleCancellation(array, canceledOrder) {
   for (let i = 0; i < array.length; i++) {
      if (array[i].order_id === canceledOrder.order_id) {
         array[i].size -= canceledOrder.size;
         if (array[i].size <= 0) {
            array.splice(i, 1);
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

      const result = {
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

      outputStream.write(JSON.stringify(result) + "\n");
   }
});

rl.on("close", () => {
   outputStream.end();
   console.log("Finished processing and writing to the file.");
});
