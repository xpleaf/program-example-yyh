package cn.xpleaf.spark.antlr;

import cn.xpleaf.spark.antlr.codegen.CalculatorLexer;
import cn.xpleaf.spark.antlr.codegen.CalculatorParser;
import org.antlr.v4.runtime.ANTLRInputStream;
import org.antlr.v4.runtime.CommonTokenStream;

/**
 * idea使用antlr4参考：
 * https://blog.csdn.net/qq_36616602/article/details/85858133
 *
 */
public class Driver {

    public static void main(String[] args) {
        String query = "3.1*(6.3-4.51)";
        CalculatorLexer lexer = new CalculatorLexer(new ANTLRInputStream(query));
        CalculatorParser parser = new CalculatorParser(new CommonTokenStream(lexer));
        MyCalculatorVisitor visitor = new MyCalculatorVisitor();
        System.out.println(visitor.visit(parser.expr()));
    }

}
