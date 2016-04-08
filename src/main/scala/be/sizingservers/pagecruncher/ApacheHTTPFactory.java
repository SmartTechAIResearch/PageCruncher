package be.sizingservers.pagecruncher; /**
 * Created by wannes on 7/3/15.
 */
import org.apache.http.*;
import org.apache.http.entity.BasicHttpEntity;
import org.apache.http.impl.DefaultHttpRequestFactory;
import org.apache.http.impl.DefaultHttpResponseFactory;
import org.apache.http.impl.entity.EntityDeserializer;
import org.apache.http.impl.entity.LaxContentLengthStrategy;
import org.apache.http.impl.io.AbstractSessionInputBuffer;
import org.apache.http.impl.io.HttpRequestParser;
import org.apache.http.impl.io.HttpResponseParser;
import org.apache.http.io.HttpMessageParser;
import org.apache.http.io.SessionInputBuffer;
import org.apache.http.message.BasicHttpEntityEnclosingRequest;
import org.apache.http.message.BasicHttpResponse;
import org.apache.http.message.BasicLineParser;
import org.apache.http.params.BasicHttpParams;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 *
 */
public class ApacheHTTPFactory {
    public static HttpRequest createRequest(final String requestAsString) {
        try {
            SessionInputBuffer inputBuffer = new AbstractSessionInputBuffer() {
                {
                    init(new ByteArrayInputStream(requestAsString.getBytes()), 10, new BasicHttpParams());
                }

                @Override
                public boolean isDataAvailable(int timeout) throws IOException {
                    throw new RuntimeException("have to override but probably not even called");
                }
            };
            HttpMessageParser parser = new HttpRequestParser(inputBuffer, new BasicLineParser(new ProtocolVersion("HTTP", 1, 1)), new DefaultHttpRequestFactory(), new BasicHttpParams());
            HttpMessage message = parser.parse();
            if (message instanceof BasicHttpEntityEnclosingRequest) {
                BasicHttpEntityEnclosingRequest request = (BasicHttpEntityEnclosingRequest) message;
                EntityDeserializer entityDeserializer = new EntityDeserializer(new LaxContentLengthStrategy());
                HttpEntity entity = entityDeserializer.deserialize(inputBuffer, message);
                request.setEntity(entity);
            }
            return (HttpRequest) message;
        } catch (IOException e) {
            throw new RuntimeException(e);
        } catch (HttpException e) {
            throw new RuntimeException(e);
        }
    }

    public static HttpResponse createResponse(final String responseAsString) {
        try {
            SessionInputBuffer inputBuffer = new AbstractSessionInputBuffer() {
                {
                    init(new ByteArrayInputStream(responseAsString.getBytes()), 10, new BasicHttpParams());
                }

                @Override
                public boolean isDataAvailable(int timeout) throws IOException {
                    throw new RuntimeException("have to override but probably not even called");
                }
            };
            HttpMessageParser parser = new HttpResponseParser(inputBuffer, new BasicLineParser(new ProtocolVersion("HTTP", 1, 1)), new DefaultHttpResponseFactory(), new BasicHttpParams());
            HttpMessage message = parser.parse();
            if (message instanceof BasicHttpResponse) {
                BasicHttpResponse request = (BasicHttpResponse) message;
                EntityDeserializer entityDeserializer = new EntityDeserializer(new LaxContentLengthStrategy());
                HttpEntity entity = entityDeserializer.deserialize(inputBuffer, message);
                request.setEntity(entity);
            }
            return (HttpResponse) message;
        } catch (IOException e) {
            throw new RuntimeException(e);
        } catch (HttpException e) {
            throw new RuntimeException(e);
        }
    }

    public static HttpResponse createResponse(final InputStream responseInputStream) {
        try {
            SessionInputBuffer inputBuffer = new AbstractSessionInputBuffer() {
                {
                    init(responseInputStream, 10, new BasicHttpParams());
                }

                @Override
                public boolean isDataAvailable(int timeout) throws IOException {
                    throw new RuntimeException("have to override but probably not even called");
                }
            };
            HttpMessageParser parser = new HttpResponseParser(inputBuffer, new BasicLineParser(new ProtocolVersion("HTTP", 1, 1)), new DefaultHttpResponseFactory(), new BasicHttpParams());
            HttpMessage message = parser.parse();
            if (message instanceof BasicHttpResponse) {
                BasicHttpResponse request = (BasicHttpResponse) message;
                EntityDeserializer entityDeserializer = new EntityDeserializer(new LaxContentLengthStrategy());
                HttpEntity entity = entityDeserializer.deserialize(inputBuffer, message);
                request.setEntity(entity);
            }
            return (HttpResponse) message;
        } catch (IOException e) {
            throw new RuntimeException(e);
        } catch (HttpException e) {
            throw new RuntimeException(e);
        }
    }
}
