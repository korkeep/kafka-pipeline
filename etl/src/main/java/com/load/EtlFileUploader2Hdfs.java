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

    public static void main(String[] args) throws Exception{
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
        };

        System.out.println("========== Real Median Household Income");

        fred.clearInputFiles("/home/ubuntu/code/bigdata-pipeline/src/main/outputs/", "household_income.csv");

        for(US_STATES state : US_STATES.values()){
            List<EtlColumnPojo> etlHouseholdIncomeData = fred.getEtlListData(FREQUENCY.YEAR, state, titleHouseholdIncome);
            fred.writeCsv2Hdfs("household_income.csv", etlHouseholdIncomeData);

            if(state.ordinal() == 0){
                fred.writeCsv2Local(true, "/home/ubuntu/code/bigdata-pipeline/src/main/outputs/", "household_income.csv", etlHouseholdIncomeData);
            } else {
                fred.writeCsv2Local(false, "/home/ubuntu/code/bigdata-pipeline/src/main/outputs/", "household_income.csv", etlHouseholdIncomeData);
            }
        };

        System.out.println("========== Total Tax Exemptions");

        fred.clearInputFiles("/home/ubuntu/code/bigdata-pipeline/src/main/outputs/", "tax_exemption.csv");

        for(US_STATES state : US_STATES.values()){
            List<EtlColumnPojo> etlTaxExemptionData = fred.getEtlListData(FREQUENCY.YEAR, state, titleTaxExemption);
            fred.writeCsv2Hdfs("tax_exemption.csv", etlTaxExemptionData);

            if(state.ordinal() == 0){
                fred.writeCsv2Local(true, "/home/ubuntu/code/bigdata-pipeline/src/main/outputs/", "tax_exemption.csv", etlTaxExemptionData);
            } else {
                fred.writeCsv2Local(false, "/home/ubuntu/code/bigdata-pipeline/src/main/outputs/", "tax_exemption.csv", etlTaxExemptionData);
            }
        };

        System.out.println("========== Civilian Labor Force");

        fred.clearInputFiles("/home/ubuntu/code/bigdata-pipeline/src/main/outputs/", "civilian_force.csv");

        for(US_STATES state : US_STATES.values()){
            List<EtlColumnPojo> etlCivilForceData = fred.getEtlListData(FREQUENCY.YEAR, state, titleLaborForce);
            fred.writeCsv2Hdfs("civilian_force.csv", etlCivilForceData);

            if(state.ordinal() == 0){
                fred.writeCsv2Local(true, "/home/ubuntu/code/bigdata-pipeline/src/main/outputs/", "civilian_force.csv", etlCivilForceData);
            } else {
                fred.writeCsv2Local(false, "/home/ubuntu/code/bigdata-pipeline/src/main/outputs/", "civilian_force.csv", etlCivilForceData);
            }
        };

        System.out.println("========== Poverty");

        fred.clearInputFiles("/home/ubuntu/code/bigdata-pipeline/src/main/outputs/", "poverty.csv");

        for(US_STATES state : US_STATES.values()){
            List<EtlColumnPojo> etlPovertyData = fred.getEtlListData(FREQUENCY.YEAR, state, titlePoverty);
            fred.writeCsv2Hdfs("poverty.csv", etlPovertyData);

            if(state.ordinal() == 0){
                fred.writeCsv2Local(true, "/home/ubuntu/code/bigdata-pipeline/src/main/outputs/", "poverty.csv", etlPovertyData);
            } else {
                fred.writeCsv2Local(false, "/home/ubuntu/code/bigdata-pipeline/src/main/outputs/", "poverty.csv", etlPovertyData);
            }
        };

        System.out.println("========== Real GDP");

        fred.clearInputFiles("/home/ubuntu/code/bigdata-pipeline/src/main/outputs/", "real_gdp.csv");

        for(US_STATES state : US_STATES.values()){
            List<EtlColumnPojo> etlRealGDPData = fred.getEtlListData(FREQUENCY.YEAR, state, titleRealGDP);
            fred.writeCsv2Hdfs("real_gdp.csv", etlRealGDPData);

            if(state.ordinal() == 0){
                fred.writeCsv2Local(true, "/home/ubuntu/code/bigdata-pipeline/src/main/outputs/", "real_gdp.csv", etlRealGDPData);
            } else {
                fred.writeCsv2Local(false, "/home/ubuntu/code/bigdata-pipeline/src/main/outputs/", "real_gdp.csv", etlRealGDPData);
            }
        };

        System.out.println("========== Unemployee Monthly");

        fred.clearInputFiles("/home/ubuntu/code/bigdata-pipeline/src/main/outputs/", "unemployee_monthly.csv");

        for(US_STATES state : US_STATES.values()){
            List<EtlColumnPojo> etlUnemployeeData = fred.getEtlListData(FREQUENCY.MONTH, state, titleUnemployee);
            fred.writeCsv2Hdfs("unemployee_monthly.csv", etlUnemployeeData);

            if(state.ordinal() == 0){
                fred.writeCsv2Local(true, "/home/ubuntu/code/bigdata-pipeline/src/main/outputs/", "unemployee_monthly.csv", etlUnemployeeData);
            } else {
                fred.writeCsv2Local(false, "/home/ubuntu/code/bigdata-pipeline/src/main/outputs/", "unemployee_monthly.csv", etlUnemployeeData);
            }
        };

        System.out.println("========== Earnings");

        for (int i = 0; i < titleEarningsList.size(); i++){
            fred.clearInputFiles("/home/ubuntu/code/bigdata-pipeline/src/main/outputs/", getFileName(titleEarningsList.get(i)));
        }

        for (int i = 0; i < titleEarningsList.size(); i++){
            for(US_STATES state : US_STATES.values()){
                List<EtlColumnPojo> etlEarningsData = fred.getEtlListData(FREQUENCY.MONTH, state, titleEarningsList.get(i));
                fred.writeCsv2Hdfs(getFileName(titleEarningsList.get(i)), etlEarningsData);
    
                if(state.ordinal() == 0){
                    fred.writeCsv2Local(true, "/home/ubuntu/code/bigdata-pipeline/src/main/outputs/", getFileName(titleEarningsList.get(i)), etlEarningsData);
                } else {
                    fred.writeCsv2Local(false, "/home/ubuntu/code/bigdata-pipeline/src/main/outputs/", getFileName(titleEarningsList.get(i)), etlEarningsData);
                }
            }
        }

        fred.closeStream();
        System.out.println("========== Done!!");
    }
}
