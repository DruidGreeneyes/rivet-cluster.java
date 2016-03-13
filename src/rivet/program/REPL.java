package rivet.program;

import java.io.BufferedReader;
import java.io.IOException;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.commons.lang3.ArrayUtils;

import scala.Tuple2;

public final class REPL {

	public static void main(String[] args) {
		try (MethodIndex methodIndex = new MethodIndex()) {
			String res;
			if (args.length > 0) {
				System.out.println("Attempting to evaluate single command...");
				res = eval(args, methodIndex)._1;
			} else {
				try (BufferedReader input = new BufferedReader(System.console().reader())) {
					System.out.println("Entering interactive environment...");
					res = repl(input, methodIndex);
				} catch (IOException e) {
					e.printStackTrace();
					res = "ERROR!";
				} 
			}
			System.out.println(res);
		}
	}

	private static String repl (final BufferedReader reader, final MethodIndex methodIndex) {
		Tuple2<String, Boolean> res = falseResult("No Input");
		do {
			System.out.print("=> ");
			try {
				res = eval(reader.readLine(), methodIndex);
			} catch (IOException e) {
				e.printStackTrace();
				res = falseResult(e.getMessage());
			}
			System.out.println(res._1);			
		} while (res._2);
		return "wHEEEEEE!";
	}
	
	private static Tuple2<String, Boolean> eval (String input, MethodIndex methodIndex) {
		input = input.trim();
		if (input.isEmpty()) return trueResult(input);
		return eval(input.split(" "), methodIndex);
	}
	
	private static Tuple2<String, Boolean> eval (String[] inputArr, MethodIndex methodIndex) {
		String cmd = inputArr[0];
		String[] args = ArrayUtils.subarray(inputArr, 1, inputArr.length);
		switch (cmd) {
			case ""    : return trueResult("Empty Command!");
			case "exit": 
			case "quit": return falseResult("Leaving now.");
			case "ls"  : return trueResult(printMethods(methodIndex));
			default    : return execute(cmd, args, methodIndex);
		}
	}
	
	private static Tuple2<String, Boolean> execute (String cmd, Object[] args, MethodIndex methodIndex) {
		System.out.println("Attempting to evaluate statement...");
		List<Method> possibles = methodIndex.methods.stream()
				.filter((m) -> m.getName().equalsIgnoreCase(cmd))
				.collect(Collectors.toList());
		if (possibles.size() < 1) 
			return trueResult(String.format("Command '%s' not found.", cmd));
		
		int exp = args.length;
		
		possibles.removeIf((m) -> m.getParameters().length != exp);
		if (possibles.size() < 1)
			return trueResult(
					String.format("Command '%s' not found accepting %d args",
							cmd, exp));
		
		Method method = possibles.get(0);
		Object res;
		try {
			res = method.invoke(methodIndex, args);
		} catch (Throwable e) {
			System.out.println(String.format("Unable to invoke method: %s with args: %s", method.getName(), ArrayUtils.toString(args)));
			System.out.println(e.getMessage());
			System.out.println(e.getCause().getMessage());
			e.printStackTrace();
			return falseResult(e.getMessage());
		}
		return trueResult(res.toString());
	}
	
	private static String printMethods(MethodIndex methodIndex) {
		StringBuilder s = new StringBuilder();
		methodIndex.methods.forEach((m) -> {
			s.append(m.getName());
			s.append(": ");
			Arrays.stream(m.getParameters())
				.forEach((p) -> {
					s.append(p.getName());
					s.append(" -> ");
				});
			s.append(m.getReturnType().getSimpleName());
			s.append('\n');
		});
		return s.toString();
	}
	
	private static Tuple2<String, Boolean> trueResult(String res) {return new Tuple2<>(res, true);}
	private static Tuple2<String, Boolean> falseResult(String res) {return new Tuple2<>(res, false);} 
}
