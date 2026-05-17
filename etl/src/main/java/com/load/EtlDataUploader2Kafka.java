package com.load;

import com.processor.Hdfs2Kafka;

public class EtlDataUploader2Kafka {

    public static void main(String[] args) throws Exception{

        Hdfs2Kafka hdfs2Kafka = new Hdfs2Kafka();

        hdfs2Kafka.readHadoopFile("unemployee_annual.csv").forEach(str ->hdfs2Kafka.sendLines2Kafka("topic_unempl_ann", str));
        hdfs2Kafka.getHadoopFilesInfo("unemployee_annual.csv");

        hdfs2Kafka.readHadoopFile("household_income.csv").forEach(str ->hdfs2Kafka.sendLines2Kafka("topic_house_income_ann", str));
        hdfs2Kafka.getHadoopFilesInfo("household_income.csv");

        hdfs2Kafka.readHadoopFile("tax_exemption.csv").forEach(str ->hdfs2Kafka.sendLines2Kafka("topic_tax_exemp_ann", str));
        hdfs2Kafka.getHadoopFilesInfo("tax_exemption.csv");

        hdfs2Kafka.readHadoopFile("civilian_force.csv").forEach(str ->hdfs2Kafka.sendLines2Kafka("topic_civil_force_ann", str));
        hdfs2Kafka.getHadoopFilesInfo("civilian_force.csv");

        hdfs2Kafka.readHadoopFile("poverty.csv").forEach(str ->hdfs2Kafka.sendLines2Kafka("topic_pov_ann", str));
        hdfs2Kafka.getHadoopFilesInfo("poverty.csv");

        hdfs2Kafka.readHadoopFile("real_gdp.csv").forEach(str ->hdfs2Kafka.sendLines2Kafka("topic_gdp_ann", str));
        hdfs2Kafka.getHadoopFilesInfo("real_gdp.csv");

        hdfs2Kafka.readHadoopFile("unemployee_monthly.csv").forEach(str ->hdfs2Kafka.sendLines2Kafka("topic_unempl_mon", str));
        hdfs2Kafka.getHadoopFilesInfo("unemployee_monthly.csv");

        hdfs2Kafka.readHadoopFile("earnings_construction.csv").forEach(str ->hdfs2Kafka.sendLines2Kafka("topic_earn_construction_mon", str));
        hdfs2Kafka.getHadoopFilesInfo("earnings_construction.csv");

        hdfs2Kafka.readHadoopFile("earnings_education_and_health_services.csv").forEach(str ->hdfs2Kafka.sendLines2Kafka("topic_earn_education_and_health_mon", str));
        hdfs2Kafka.getHadoopFilesInfo("earnings_education_and_health_services.csv");

        hdfs2Kafka.readHadoopFile("earnings_financial_activites.csv").forEach(str ->hdfs2Kafka.sendLines2Kafka("topic_earn_financial_activities_mon", str));
        hdfs2Kafka.getHadoopFilesInfo("earnings_financial_activites.csv");

        hdfs2Kafka.readHadoopFile("earnings_goods_producing.csv").forEach(str ->hdfs2Kafka.sendLines2Kafka("topic_earn_goods_producing_mon", str));
        hdfs2Kafka.getHadoopFilesInfo("earnings_goods_producing.csv");

        hdfs2Kafka.readHadoopFile("earnings_leisure_and_hospitality.csv").forEach(str ->hdfs2Kafka.sendLines2Kafka("topic_earn_leisure_and_hospitality_mon", str));
        hdfs2Kafka.getHadoopFilesInfo("earnings_leisure_and_hospitality.csv");

        hdfs2Kafka.readHadoopFile("earnings_manufacturing.csv").forEach(str ->hdfs2Kafka.sendLines2Kafka("topic_earn_manufacturing_mon", str));
        hdfs2Kafka.getHadoopFilesInfo("earnings_manufacturing.csv");

        hdfs2Kafka.readHadoopFile("earnings_private_service_providing.csv").forEach(str ->hdfs2Kafka.sendLines2Kafka("topic_earn_-private_service_providing_mon", str));
        hdfs2Kafka.getHadoopFilesInfo("earnings_private_service_providing.csv");

        hdfs2Kafka.readHadoopFile("earnings_professional_and_business_services.csv").forEach(str ->hdfs2Kafka.sendLines2Kafka("topic_professional_and_business_services_mon", str));
        hdfs2Kafka.getHadoopFilesInfo("earnings_professional_and_business_services.csv");

        hdfs2Kafka.readHadoopFile("earnings_trade_transportation_and_utilities.csv").forEach(str ->hdfs2Kafka.sendLines2Kafka("topic_earn_trade_transportation_and_utilities_mon", str));
        hdfs2Kafka.getHadoopFilesInfo("earnings_trade_transportation_and_utilities.csv");

        hdfs2Kafka.closeStream();
    }
}
