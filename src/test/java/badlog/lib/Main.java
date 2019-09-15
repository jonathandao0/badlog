package badlog.lib;

public class Main {
	static BadLog log;

	public static void main(String[] args) {
		log = BadLog.init("test.bag");
		{
			BadLog.createValue("Example Value", System.getProperty("os.version"));
			
			BadLog.createTopic("Example Topic", "Bytes", () -> (double) Runtime.getRuntime().freeMemory());

			BadLog.createTopic("Topic with attributes", BadLog.UNITLESS, () -> 3.2, "attr1", "attr2");
			
			BadLog.createTopicSubscriber("Subscribed topic", "s", DataInferMode.DEFAULT);
		}
		log.finishInitialization();
		ThreadedLog logger = new ThreadedLog();
		logger.start();

		BadLog.stop();
		log = BadLog.init("test2.bag");
		{
			BadLog.createValue("Example Value", System.getProperty("os.version"));

			BadLog.createTopic("Example Topic", "Bytes", () -> (double) Runtime.getRuntime().freeMemory());

			BadLog.createTopic("Topic with attributes", BadLog.UNITLESS, () -> 3.2, "attr1", "attr2");

			BadLog.createTopicSubscriber("Subscribed topic", "s", DataInferMode.DEFAULT);
		}
		log.finishInitialization();

		for (int i = 0; i < 10; i++) {
			// Publish to subscribed topic
			BadLog.publish("Subscribed topic", (double) System.nanoTime());

			log.updateTopics();
			log.log();
		}
	}

	public static class ThreadedLog extends Thread {
		@Override
		public void run() {
			for (int i = 0; i < 10; i++) {
				// Publish to subscribed topic
				BadLog.publish("Subscribed topic", (double) System.nanoTime());

				log.updateTopics();
				log.log();
			}
		}
	}
}
