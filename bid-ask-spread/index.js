const fs = require("fs");
const readline = require("readline");

const fileStream = fs.createReadStream(
   "../stock-data/smci/XNAS-20240217-DR3J9CCF3H/xnas-itch-20240205.mbo.json"
);

const outputStream = fs.createWriteStream("./outputs/jsOutput.json");

const rl = readline.createInterface({
   input: fileStream,
   crlfDelay: Infinity,
});

// define variables to store the best bids and asks
const bestBids = [];
const bestAsks = [];
const ARRAY_MAX_LENGTH = 4;

// update or insert into array, based on price
const updateOrInsert = (array, newOrder, compareFn) => {
   let orderExists = false;
   // look for the price in the array
   for (let i = 0; i < array.length; i++) {
      if (array[i].price === newOrder.price) {
         array[i].size += newOrder.size;
         orderExists = true;
         break;
      }
   }
   // if order doesn't exist, insert it in the array
   !orderExists && insertSorted(array, newOrder, compareFn);
   // remove last item if array is too large
   array.length > ARRAY_MAX_LENGTH && array.pop();
};

// insert into sorted array
const insertSorted = (array, element, compareFn) => {
   // default position is the end of the array
   let insertionIndex = array.length;
   // find the correct position using the compare function
   for (let i = 0; i < array.length; i++) {
      if (compareFn(element, array[i]) < 0) {
         insertionIndex = i;
         break;
      }
   }
   array.splice(insertionIndex, 0, element);
};

// remove or reduce size of order in the array
const handleCancellation = (array, canceledOrder) => {
   for (let i = 0; i < array.length; i++) {
      if (array[i].order_id === canceledOrder.order_id) {
         array[i].size -= canceledOrder.size;
         if (array[i].size <= 0) {
            array.splice(i, 1);
         }
         break;
      }
   }
};

rl.on("line", (line) => {
   const order = JSON.parse(line);

   // handle new orders
   if (order.action === "A") {
      let price = parseInt(order.price, 10) / 1e9;
      let size = parseInt(order.size, 10);

      // handle asks
      if (order.side === "A") {
         updateOrInsert(
            bestAsks,
            { price, size, order_id: order.order_id },
            (a, b) => a.price - b.price
         );
      }
      // handle bids
      else if (order.side === "B") {
         updateOrInsert(
            bestBids,
            { price, size, order_id: order.order_id },
            (a, b) => b.price - a.price
         );
      }

      // create the order object
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
   // handle cancellations ("C" - cancel actions)
   else if (order.action === "C") {
      if (order.side === "A") {
         handleCancellation(bestAsks, order);
      } else if (order.side === "B") {
         handleCancellation(bestBids, order);
      }
   }
});

rl.on("close", () => {
   outputStream.end();
   console.log("Finished processing and writing to the file.");
});
