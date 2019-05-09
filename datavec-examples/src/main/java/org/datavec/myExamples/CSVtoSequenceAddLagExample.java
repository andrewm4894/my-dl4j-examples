package org.datavec.myExamples;

import org.datavec.api.records.reader.RecordReader;
import org.datavec.api.records.reader.impl.csv.CSVRecordReader;
import org.datavec.api.split.FileSplit;
import org.datavec.api.transform.TransformProcess;
import org.datavec.api.transform.schema.Schema;
import org.datavec.api.transform.sequence.comparator.NumericalColumnComparator;
import org.datavec.api.transform.transform.sequence.SequenceOffsetTransform;
import org.datavec.api.writable.Writable;
import org.datavec.local.transforms.LocalTransformExecutor;
import org.joda.time.DateTimeZone;
import org.nd4j.linalg.io.ClassPathResource;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class CSVtoSequenceAddLagExample {

    public static void main(String[] args) throws  Exception {

        Schema inputDataSchema = new Schema.Builder()
            .addColumnString("DateTimeString")
            .addColumnsString("CustomerID", "MerchantID")
            .addColumnInteger("NumItemsInTransaction")
            .addColumnCategorical("MerchantCountryCode", Arrays.asList("USA","CAN","FR","MX"))
            .addColumnDouble("TransactionAmountUSD",0.0,null,false,false)   //$0.0 or more, no maximum limit, no NaN and no Infinite values
            .addColumnCategorical("FraudLabel", Arrays.asList("Fraud","Legit"))
            .build();

        TransformProcess tp = new TransformProcess.Builder(inputDataSchema)
            .removeAllColumnsExceptFor("CustomerID","DateTimeString","TransactionAmountUSD")
            .stringToTimeTransform("DateTimeString","YYYY-MM-DD HH:mm:ss.SSS", DateTimeZone.UTC)
            .convertToSequence(Arrays.asList("CustomerID"), new NumericalColumnComparator("DateTimeString"))
            .offsetSequence(Arrays.asList("TransactionAmountUSD"),1, SequenceOffsetTransform.OperationType.NewColumn)
            .build();

        File inputFile = new ClassPathResource("BasicDataVecExample/exampledata.csv").getFile();

        //Define input reader and output writer:
        RecordReader rr = new CSVRecordReader(0, ',');
        rr.initialize(new FileSplit(inputFile));

        //Process the data:
        List<List<Writable>> originalData = new ArrayList<>();
        while(rr.hasNext()){
            originalData.add(rr.next());
        }

        List<List<List<Writable>>> processedData = LocalTransformExecutor.executeToSequence(originalData, tp);

        System.out.println("=== BEFORE ===");

        for (int i=0;i<originalData.size();i++) {
            System.out.println(originalData.get(i));
        }

        System.out.println("=== AFTER ===");
        for (int i=0;i<processedData.size();i++) {
            System.out.println(processedData.get(i));
        }

    }
    
}
