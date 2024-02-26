// ignore this file - not working

const fs = require("fs");
const path = require("path");
const { Transform, pipeline } = require("stream");

const sourceDir = "../stock-data/smci/XNAS-20240217-DR3J9CCF3H"; // Replace with your actual directory path
const outputFilename = "./outputs/combinedData.json";
const filesPattern = /^xnas-itch-(\d{8})\.mbo\.json$/; // Regex to match your files with a capturing group for the date

// Create a writable stream for the output file
const outputStream = fs.createWriteStream(outputFilename, { flags: "a" });

// read and parse a JSON file into a stream
const processFile = (filePath, doneCallback) => {
   const inputStream = fs.createReadStream(filePath, "utf-8");

   const jsonTransform = new Transform({
      readableObjectMode: true,
      writableObjectMode: true,
      transform(chunk, encoding, callback) {
         const lines = chunk.split(/\r?\n/);
         lines.forEach((line) => {
            if (line) {
               try {
                  const json = JSON.parse(line);
                  this.push(JSON.stringify(json) + "\n");
               } catch (e) {
                  console.error(
                     `Error parsing JSON in file ${filePath}, line: ${line}: ${e}`
                  );
               }
            }
         });
         callback();
      },
   });

   pipeline(inputStream, jsonTransform, outputStream, (err) => {
      if (err) {
         console.error("Pipeline failed.", err);
      } else {
         console.log(`File ${filePath} processed.`);
      }
      doneCallback(err);
   });
};

// Function to extract date from filename
const extractDate = (filename) => {
   const match = filesPattern.exec(filename);
   return match ? match[1] : null;
};

// Function to compare filenames based on their dates
const byDate = (a, b) => {
   const dateA = extractDate(a);
   const dateB = extractDate(b);
   return dateA.localeCompare(dateB);
};

// Read the directory and process each file in date order
fs.readdir(sourceDir, (err, files) => {
   if (err) {
      console.error("Error reading directory:", err);
      return;
   }

   const sortedFiles = files
      .filter((file) => filesPattern.test(file))
      .sort(byDate);

   let i = 0;
   const processNext = () => {
      if (i < sortedFiles.length) {
         processFile(path.join(sourceDir, sortedFiles[i]), (err) => {
            if (!err) {
               i++;
               processNext();
            }
         });
      } else {
         outputStream.end();
         console.log(`Combined data written to ${outputFilename}`);
      }
   };

   processNext();
});
