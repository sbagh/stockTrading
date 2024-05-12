// analyze earnings
// group into 2 categories: goodEarnings and badEarnings
// define what is good and bad earnings
// criteria based on categories in this order:
// guidance (4 points)
// revenue growth (3 points)
// EPS compared to expectations (3 points)
// profit growth (2 points)
// key metrics (e.g. user growth, churn rate, etc.) (1 point)
// compparative analysis with competitors (1 point)
// news surrounding the earnings (1 point)

import fetch from "node-fetch";
import fs from "fs";

const API_KEY = "";
const INCOME_STATEMENT_URL = `https://financialmodelingprep.com/api/v3`;

const getEarnings = async (ticker, period) => {
   const path = `/income-statement/${ticker}?period=${period}&apikey=${API_KEY}`;

   try {
      const response = await fetch(INCOME_STATEMENT_URL + path);

      if (response.status !== 200) {
         throw new Error(
            `Failed to fetch earnings data with status: ${response.status}`
         );
      }

      return response.json();
   } catch (error) {
      console.error("Error fetching earnings data:", error);
      return null;
   }
};

getEarnings("AAPL", "quarter")
   .then((data) => {
      console.log(data);
   })
   .catch((error) => {
      console.error("Error getting earnings data:", error);
   });

const analyzeEarnings = async (
   ticker,
   recentEarningsYear,
   previousEarnings
) => {};
