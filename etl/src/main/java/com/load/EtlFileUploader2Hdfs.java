package com.load;

import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.thirdparty.org.checkerframework.checker.units.qual.s;

import com.pojo.EtlColumnPojo;
import com.processor.Fred2Hdfs;
import com.processor.Fred2Hdfs.FREQUENCY;
import com.util.US_STATES;

public class EtlFileUploader2Hdfs {
    
    private static List<String> titleEarningsList = Arrays.asList(
        "Average Hourly Earnings of all Employees: Constructed in ",
        "Average Hourly Earnings of all Employees: Education and Health Services in ",
        "Average Hourly Earnings of all Employees: Financial Producing in ",
        "Average Hourly Earnings of all Employees: Goods Producing in ",
        "Average Hourly Earnings of all Employees: Leisure and Hospitality in ",
        "Average Hourly Earnings of all Employees: Manufacturing in ",
        "Average Hourly Earnings of all Employees: Private Service Providing in ",
        "Average Hourly Earnings of all Employees: Professional and Business Services in ",
        "Average Hourly Earnings of all Employees: Trade Transportation and Utilities in "
    );

    private static String titlePoverty = "Estimated Percent of People of All Ages in Poverty for ";
    private static String titleRealGDP = "Real Gross Domestic Product: All Industry Total in";
    private static String titleUnemployee = "Unemployment Rate in ";
    private static String titleHouseholdIncome = "Real Median Household Income in";
    private static String titleTaxExemption = "Total tax Exemptions for";
    private static String titleLaborForce = "Civilian Labor Force in";
    
    public static String getFileName(String str){
        int istart = str.indexOf(":") + 2;
        int iend = str.indexOf(" in");
        String title = str.substring(istart, iend).replace(" ", "_").replace(",", "");
        String fileName = "earnings_" + title + ".csv";
        return fileName;
    }

    public static void main(Strings[] args) throws Exception{
        Fred2Hdfs fred = new Fred2Hdfs();

        System.out.println("========== Unemployee Annual");
        
        fred.clearInputFiles("/home/ubuntu/code/bigdata-pipeline/src/main/outputs/", "unemployee_annual.csv");
        
        for(US_STATES state : US_STATES.values()){
            List<EtlColumnPojo> etlUnemployeeDataAnnual = fred.getEtlListData(FREQUENCY.YEAR, state, titleUnemployee);
            fred.writeCsv2Hdfs("unemployee_annual.csv", etlUnemployeeDataAnnual);

            if(state.ordinal() == 0){
                fred.writeCsv2Local(true, "/home/ubuntu/code/bigdata-pipeline/src/main/outputs/", "unemployee_annual.csv", etlUnemployeeDataAnnual);
            } else {
                fred.writeCsv2Local(false, "/home/ubuntu/code/bigdata-pipeline/src/main/outputs/", "unemployee_annual.csv", etlUnemployeeDataAnnual);
            }

            // TODO
        };

    }



}
