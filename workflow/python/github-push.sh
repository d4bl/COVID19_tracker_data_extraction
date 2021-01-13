#echo "Results from $(date +%Y-%m-%d--%H:%M:%S) run" > /tmp/null
#export timestamp=$(cat /tmp/null)

commit_msg=$(echo "Results from $(date +%B_%d,_%Y--%H:%M:%S) run")
echo $commit_msg

git add output/csv/covid_disparities_output_*
git add output/xlsx/covid_disparities_output_*
git add output/master-table/combinedData*
git add output/latest-combined-output.csv
git add output/latest-single-day-output.csv
#git add run_scrapers.log

git commit -m "${commit_msg}"
git push origin master


