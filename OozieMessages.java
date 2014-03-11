import java.io.IOException;
import java.util.Properties;
import java.util.Scanner;

import javax.naming.*;
import javax.jms.*;

import org.apache.oozie.AppType;
import org.apache.oozie.client.JMSConnectionInfo;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.client.AuthOozieClient;
import org.apache.oozie.client.OozieClientException;
import org.apache.oozie.client.event.Event.MessageType;
import org.apache.oozie.client.event.jms.JMSHeaderConstants;
import org.apache.oozie.client.event.jms.JMSMessagingUtils;
import org.apache.oozie.client.event.message.SLAMessage;
import org.apache.oozie.client.event.message.WorkflowJobMessage;
import org.apache.hadoop.security.authentication.client.Authenticator;
import com.yahoo.oozie.security.authentication.client.KerberosAuthenticator;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;


public class OozieMessages implements MessageListener {

    String url, topicStr, ycaRole;
    public static void main(String args[]) {
        try {
            OozieMessages m = new OozieMessages();
            m.url = args[0];
            m.topicStr = args[1];
            m.ycaRole = args[2];
            m.consumeMessages();
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void consumeMessages() throws OozieClientException, JMSException, NamingException, InterruptedException {

        KerbOozieClient oc = new KerbOozieClient(url);
        JMSConnectionInfo jmsInfo = oc.getJMSConnectionInfo();
        Properties jndiProperties = jmsInfo.getJNDIProperties();
        jndiProperties.setProperty("java.naming.security.principal", ycaRole);
        Context jndiContext = new InitialContext(jndiProperties);
        System.out.println("*** [DEBUG] jndiContext properties: " + jndiContext.getEnvironment().toString());
        String connectionFactoryName = (String) jndiContext.getEnvironment().get("connectionFactoryNames");
        ConnectionFactory connectionFactory = (ConnectionFactory) jndiContext.lookup(connectionFactoryName);
        Connection connection = connectionFactory.createConnection();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        String topicPrefix = jmsInfo.getTopicPrefix();
        String topicPattern = jmsInfo.getTopicPattern(AppType.WORKFLOW_JOB);
        // Following code checks if the topic pattern is
        // 'username', then the topic name is set to the actual user submitting
        // the job
        String topicName = null;
        if (topicPattern.equals("${username}")) {
            topicName = topicStr;
        }
        Destination topic = session.createTopic(topicPrefix + topicName);
        MessageConsumer consumer = session.createConsumer(topic);
        consumer.setMessageListener(this);
        connection.start();
        System.out.println("*** Listener started......");
        // keep enough time to establish connection
        Thread.sleep(60 * 1000);
        System.out.println("*** Submit job now.....");
        Thread.sleep(120 * 1000);
        Scanner sc = new Scanner(System.in);
        System.out.println("*** Type 'exit' to stop listener....");
        while(true) {
            if (sc.nextLine().equalsIgnoreCase("exit")) {
                System.exit(0);
            }
        }
    }

    @Override
    public void onMessage(Message message) {
        try {
            if (message.getStringProperty(JMSHeaderConstants.MESSAGE_TYPE).equals(MessageType.SLA.name())) {
                SLAMessage slaMessage = JMSMessagingUtils.getEventMessage(message);
                System.out.println("*** [Message]: " + slaMessage.getSLAStatus());
            }
            else if (message.getStringProperty(JMSHeaderConstants.APP_TYPE).equals(AppType.WORKFLOW_JOB.name())) {
                WorkflowJobMessage wfJobMessage = JMSMessagingUtils.getEventMessage(message);
                System.out.println("*** [Message]: " + wfJobMessage.getEventStatus());
            }
        }
        catch (JMSException jmse) {
            jmse.printStackTrace();
        }
        catch (IOException ioe) {
            ioe.printStackTrace();
        }
    }

static class KerbOozieClient extends AuthOozieClient {

    public KerbOozieClient(String oozieUrl) {
        super(oozieUrl, "KERBEROS");
    }

    @Override
    protected Map<String, Class<? extends Authenticator>> getAuthenticators() {
        Map<String, Class<? extends Authenticator>> authClasses = new HashMap<String, Class<? extends Authenticator>>();
        authClasses.put("KERBEROS", KerberosAuthenticator.class);
        return authClasses;
    }

}

}

