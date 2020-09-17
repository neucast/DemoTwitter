package org.banxico.dgef;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Luis Neumann.
 * Ejemplo de Stream desde Twitter.
 */
public class App {

    /**
     * Crea un cliente de twitter y almacena los mensajes que contienen las palabras buscadas.
     *
     * @param consumerKey      String
     * @param consumerSecret   String
     * @param token            String
     * @param secret           String
     * @param numberOfMessages int
     * @throws InterruptedException
     */
    public static void run(String consumerKey, String consumerSecret, String token, String secret, int numberOfMessages)
            throws InterruptedException {

        BlockingQueue<String> queue = new LinkedBlockingQueue<String>(10000);
        StatusesFilterEndpoint endpoint = new StatusesFilterEndpoint();

        // Terminos o palabras a buscar.
        endpoint.trackTerms(Lists.newArrayList("amlo", "covid", "pandemia", "trump", "biden", "AMLO", "COVID-19", "COVID", "Trump", "Biden"));

        Authentication auth = new OAuth1(consumerKey, consumerSecret, token, secret);
        // Authentication auth = new BasicAuth(username, password);

        // Crea un nuevo cliente de tipo básico. Por default gzip esta habilitado.
        Client client = new ClientBuilder().hosts(Constants.STREAM_HOST).endpoint(endpoint).authentication(auth)
                .processor(new StringDelimitedProcessor(queue)).build();

        // Establece la conexión.
        client.connect();

        // Ruta de archivo de salida.
        String filePath = "D:\\tweetOutput\\tweetOutput.txt";

        // Lee los mensajes del stream.
        for (int msgRead = 0; msgRead < numberOfMessages; msgRead++) {
            String msg = queue.take();
            System.out.println("Se encontró el twitter #" + msgRead + ".");

            // Almacema los mensajes encontrados en el archivo de salida.
            appendToFile(filePath, msg);

        }

        // Detiene y cierra la conexión con el cliente.
        client.stop();

    }

    /**
     * Escribe en el archivo solicitado.
     *
     * @param filePath String
     * @param text     String
     */
    private static void appendToFile(String filePath, String text) {
        PrintWriter fileWriter = null;

        try {
            fileWriter = new PrintWriter(new BufferedWriter(new FileWriter(filePath, true)));
            fileWriter.println(text);
        } catch (IOException ioException) {
            ioException.printStackTrace();
        } finally {
            if (fileWriter != null) {
                fileWriter.close();
            }
        }
    }

    /**
     * Método inicial de ejecución.
     *
     * @param args String[]
     */
    public static void main(String[] args) {

        try {
            String consumerKey = "";
            String consumerSecret = "";
            String token = "";
            String secret = "";
            App.run(consumerKey, consumerSecret, token, secret, 1000);
        } catch (InterruptedException e) {
            System.out.println(e);
        }
    }
}