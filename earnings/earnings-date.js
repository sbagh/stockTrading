const https = require("https");
const fs = require("fs");

const API_KEY = "";

const startDate = "2024-04-07";
const endDate = "2024-08-10";

const options = {
   hostname: "financialmodelingprep.com",
   port: 443,
   path: `https://financialmodelingprep.com/api/v3/earning_calendar?from=${startDate}&to=${endDate}&apikey=${API_KEY}`,
   method: "GET",
};

const req = https.request(options, (res) => {
   let data = "";

   res.on("data", (chunk) => {
      data += chunk;
   });

   res.on("end", () => {
      try {
         const jsonData = JSON.parse(data);
         fs.writeFile(
            "earningsCalendar.json",
            JSON.stringify(jsonData, null, 2),
            (err) => {
               if (err) {
                  console.error("Error writing file:", err);
               } else {
                  console.log("Data written to file successfully.");
               }
            }
         );
      } catch (error) {
         console.error("Error parsing JSON!", error);
      }
   });
});

req.on("error", (error) => {
   console.error("Error with request:", error);
});

req.end();
