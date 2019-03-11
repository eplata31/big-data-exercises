package nearsoft.academy.bigdata.recommendation;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.Writer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.zip.GZIPInputStream;

import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.impl.model.file.FileDataModel;
import org.apache.mahout.cf.taste.impl.neighborhood.ThresholdUserNeighborhood;
import org.apache.mahout.cf.taste.impl.recommender.GenericUserBasedRecommender;
import org.apache.mahout.cf.taste.impl.similarity.PearsonCorrelationSimilarity;
import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.cf.taste.neighborhood.UserNeighborhood;
import org.apache.mahout.cf.taste.recommender.RecommendedItem;
import org.apache.mahout.cf.taste.recommender.UserBasedRecommender;
import org.apache.mahout.cf.taste.similarity.UserSimilarity;

/**
 * MovieRecommender
 */
public class MovieRecommender {

    private int totalReviews;
    private HashMap<String, Integer> users;
    private HashMap<String, Integer> products;
    private String createdCsv;

    public MovieRecommender(String dataSource) throws IOException, TasteException {
        GenericUserBasedRecommender recommender;
        createdCsv = convertToCsv(dataSource);
        DataModel model = new FileDataModel(new File(createdCsv));
        UserSimilarity similarity = new PearsonCorrelationSimilarity(model);
        UserNeighborhood neighborhood = new ThresholdUserNeighborhood(0.1, similarity, model);
        recommender = new GenericUserBasedRecommender(model, neighborhood, similarity);
    }

    private String convertToCsv(String dataFile){
        try {

            File source = new File(dataFile);
            users = new HashMap<String,Integer>();
            products = new HashMap<String,Integer>();

            File dataInCsv = new File(source.getParentFile().getAbsolutePath() + "/dataInCsv.csv");
            if (dataInCsv.exists()) {
                dataInCsv.delete();
            }
            else{
                dataInCsv.createNewFile();
            }

            InputStream fileStream = new FileInputStream(dataFile);
            InputStream gzipStream = new GZIPInputStream(fileStream);
            Reader decoder = new InputStreamReader(gzipStream, "UTF-8");
            BufferedReader buffered = new BufferedReader(decoder);
            Writer writer = new BufferedWriter(new FileWriter(dataInCsv));
            String line;
            Integer userId = null;
            String score = "";
            Integer productId = null;
            totalReviews = 0;
            while ((line = buffered.readLine()) != null) {
                if (line.contains("product/productId")) {
                    String productName = getValue(line);
                    if(!products.containsKey(productName)){
                        products.put(productName,products.size()+1);
                    }
                    productId = products.get(productName);
                    totalReviews++;
                } else if (line.contains("review/userId")) {
                    String userName =getValue(line);
                    if(!users.containsKey(userName)){
                        users.put(userName,users.size()+1);
                    }
                    userId = users.get(userName);
                } else if (line.contains("review/score")) {
                    score = getValue(line);
                    writer.append(String.valueOf(userId));
                    writer.append(",");
                    writer.append(String.valueOf(productId));
                    writer.append(",");
                    writer.append(score);
                    writer.append("\n");
                    
                    userId = null;
                    score = "";
                    productId = null;
                }
            }
            buffered.close();
            writer.close();
            System.out.println("data.csv file: " + dataInCsv.getAbsolutePath());
            return dataInCsv.getAbsolutePath();
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException("Error processing the data source " + dataFile, e);
        }
    }

    private String getValue(String line) {
        return line.substring(line.indexOf(":") + 2, line.length());
    }

    public int getTotalProducts(){
        return products.size();
    }

    public int getTotalUsers(){
        return users.size();
    }


    public int getTotalReviews() {
        return totalReviews;
    }

    public int searchUser(String user){
        int idUser = users.get(user);
        return idUser;

    }

    public String getProductID(int value){
        Iterator<String> keys = products.keySet().iterator();
        while(keys.hasNext()){
            String key = keys.next();
            if (products.get(key)==value){
                return key;
            }
        }
        return null;
}

    public List<String> getRecommendationsForUser(String user) throws TasteException, IOException {
        DataModel model = new FileDataModel(new File(createdCsv));
        UserSimilarity similarity = new PearsonCorrelationSimilarity(model);
        UserNeighborhood neighborhood = new ThresholdUserNeighborhood(0.1, similarity, model);
        UserBasedRecommender recommender = new GenericUserBasedRecommender(model, neighborhood, similarity);
        int idUser = searchUser(user);
        List<RecommendedItem> recommendations = recommender.recommend(idUser, 3);
        List<String> output = new ArrayList<String>();
        for (RecommendedItem recommendation : recommendations) {
            output.add(getProductID((int)recommendation.getItemID()));
        }
        return output;
}
}