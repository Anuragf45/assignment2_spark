package com.example.demo.controller;

import com.example.demo.service.SparkService;
import org.apache.spark.sql.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.Resource;
import org.springframework.core.io.ResourceLoader;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

import java.io.File;
import java.io.IOException;

import static org.apache.spark.sql.functions.col;
import org.apache.commons.io.FileUtils;



@RestController
//@RequestMapping("/spark")
public class SparkController {
    private static final Logger logger = LoggerFactory.getLogger(SparkController.class);

    @Autowired
    private ResourceLoader resourceLoader;
    @Autowired
    private SparkService sparkService;

    @Autowired
    private SparkSession sparkSession;


    public static String check() {

        File directoryPath = new File("/Users/anuragsrivastava/Downloads/data-mapping-master/src/main/resources/file/");
        //List of all files and directories
        String contents[] = directoryPath.list();
       // System.out.println("List of files and directories in the specified directory:");
        for (int i = 0; i < contents.length; i++) {
           // System.out.println(contents[i]);
            if(contents[i].endsWith("csv")==true){
                //System.out.println(contents[i]);
                return contents[i];

            }
            //System.out.println(contents[i]);
        }
        return "not found";
    }


    @PostMapping("/csv")
    public ResponseEntity<?> readCsv(@RequestParam(required = false,name = "file") MultipartFile file,
                                     @RequestParam(required = false) String column,
                                     @RequestParam(required = false) String gt,
                                     @RequestParam(required = false) String lt,
                                     @RequestParam(required = false) String removeColumn,
                                     @RequestParam(required = false) String val
                                     ) throws IOException {
        if(file.isEmpty()) {
            return new ResponseEntity<>("No file found.Please upload a file",HttpStatus.BAD_REQUEST);
        }




                FileUtils.deleteDirectory(new File("/Users/anuragsrivastava/Downloads/data-mapping-master/src/main/resources/file"));
                String fileName = file.getOriginalFilename();
                Dataset<Row> dataset=sparkSession.read()
                .option("header", "true")
                .csv("/Users/anuragsrivastava/Downloads/data-mapping-master/src/main/resources/"+fileName);






        Dataset<Row> colDet=dataset;


        if(column!=null){
            colDet=  dataset.select(col(column));
        } if(column!=null && lt!=null && gt!=null){
            colDet= dataset.select(col(column).lt(lt).gt(gt));
        } if(column!=null && gt!=null && lt==null){
            colDet= dataset.select(col(column).gt(gt));
        } if(removeColumn!=null){
            colDet=  dataset.drop(removeColumn);
        } if(column!=null && lt!=null && gt==null){
            colDet=dataset.select(col(column)).where(col(column).lt(lt));
   }if(column!=null && val!=null){
            colDet=dataset.where(col(column).equalTo(val));

        }

//Dataset<Row> colDet=dataset.filter(column);
        colDet.write()
                .option("header", "true")
                .option("delimiter", ",")
                .csv("/Users/anuragsrivastava/Downloads/data-mapping-master/src/main/resources/file");




        Resource resource = resourceLoader.getResource("file:/Users/anuragsrivastava/Downloads/data-mapping-master/src/main/resources/file/"+check());

        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.parseMediaType("text/csv"));
        headers.setContentDispositionFormData("attachment", ""+check() );
        headers.setCacheControl("must-revalidate, post-check=0, pre-check=0");



        // return the response entity
        ResponseEntity<Resource> response = new ResponseEntity<>(resource, headers, HttpStatus.OK);


        return response;

    }

    @GetMapping("/download-csv")
    public ResponseEntity<Resource> downloadCsv(@RequestParam(required = false) String column) throws IOException {
        // load the CSV file from the file system
        Resource resource = resourceLoader.getResource("file:/Users/anuragsrivastava/Downloads/data-mapping-master/src/main/resources/test.csv");


        // set the headers
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.parseMediaType("text/csv"));
        headers.setContentDispositionFormData("attachment", "data.csv");
        headers.setCacheControl("must-revalidate, post-check=0, pre-check=0");



        // return the response entity
        ResponseEntity<Resource> response = new ResponseEntity<>(resource, headers, HttpStatus.OK);
        System.out.println(response);
        return response;
    }
    }
