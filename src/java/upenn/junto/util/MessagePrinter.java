package upenn.junto.util;

public class MessagePrinter {

  public static void Print (String msg) {
    System.out.print (msg + "\n");
  }
	
  public static void PrintAndDie(String msg) {
    System.out.println(msg + "\n");
    System.exit(1);
  }
}
