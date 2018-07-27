package com.Espire.FinalRegModel;

import java.io.IOException;
import java.util.ArrayList;
import java.util.StringTokenizer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class RegReducer extends Reducer<Text, Text, Text, Text> {

	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {

		ArrayList<Integer> y = new ArrayList<Integer>();
		ArrayList<Integer> x1 = new ArrayList<Integer>();
		ArrayList<Integer> x2 = new ArrayList<Integer>();
		ArrayList<Float> SST = new ArrayList<Float>();
		ArrayList<Float> SSR = new ArrayList<Float>();
		ArrayList<Float> Ycap = new ArrayList<Float>();

		int Ysum = 0, X1sum = 0, X2sum = 0, X1squaresum = 0, X2squaresum = 0, Ysquaresum = 0, X1Ysum = 0, X2Ysum = 0, X1X2sum = 0;
		float Ymean = 0f, X1mean = 0f, X2mean = 0f;

		float summationx1y = 0f, summationx2y = 0f, summationx1x2 = 0f, summationx1square = 0f, summationx2square = 0f, summationysquare = 0f;

		float BetaZero = 0f, BetaOne = 0f, BetaTwo = 0f;
		String line = " ";
		StringTokenizer st;
		for (Text value : values) {
			line = value.toString();
			st = new StringTokenizer(line, "#");
			y.add(Integer.parseInt(st.nextToken()));
			x1.add(Integer.parseInt(st.nextToken()));
			x2.add(Integer.parseInt(st.nextToken()));

		}

		int n1 = y.size();

		for (int i = 0; i < n1; i++) {
			Ysum += y.get(i);
			Ysquaresum += y.get(i) * y.get(i);

			X1sum += x1.get(i);
			X1squaresum += x1.get(i) * x1.get(i);

			X2sum += x2.get(i);
			X2squaresum += x2.get(i) * x2.get(i);

			X1Ysum += x1.get(i) * y.get(i);
			X2Ysum += x2.get(i) * y.get(i);
			X1X2sum += x1.get(i) * x2.get(i);

		}
		Ymean = (float) Ysum / n1;
		X1mean = (float) X1sum / n1;
		X2mean = (float) X2sum / n1;

		summationx1y = (float) ((float) (X1Ysum - (float) (X1sum * Ysum) / n1));
		summationx2y = (float) ((float) (X2Ysum - (float) (X2sum * Ysum) / n1));
		summationx1x2 = (float) ((float) (X1X2sum - (float) (X1sum * X2sum)
				/ n1));

		summationx1square = (float) ((float) (X1squaresum - (float) (X1sum * X1sum)
				/ n1));
		summationx2square = (float) ((float) (X2squaresum - (float) (X2sum * X2sum)
				/ n1));
		summationysquare = (float) ((float) (Ysquaresum - (float) (Ysum * Ysum)
				/ n1));

		float z = (summationx1square * summationx2square)
				- (summationx1x2 * summationx1x2);

		BetaOne = ((summationx2square * summationx1y) - (summationx1x2 * summationx2y))
				/ z;

		BetaTwo = ((summationx1square * summationx2y) - (summationx1x2 * summationx1y))
				/ z;

		BetaZero = Ymean - (BetaOne * X1mean) - (BetaTwo * X2mean);

		for (int i = 0; i < n1; i++) {
			float val1 = BetaZero + BetaTwo * (float) x2.get(i) + BetaOne
					* (float) x1.get(i);
			Ycap.add(i, val1);
			float val2 = ((float) y.get(i) - Ymean)
					* ((float) y.get(i) - Ymean);
			SST.add(i, val2);
			float val3 = (Ycap.get(i) - (float) y.get(i))
					* (Ycap.get(i) - (float) y.get(i));
			SSR.add(i, val3);
		}

		float sumSSR = 0f, sumSST = 0f;

		for (int i = 0; i < n1; i++) {
			sumSST += SST.get(i);
			sumSSR += SSR.get(i);
		}
		sumSSR = (float) sumSSR / n1;
		sumSST = (float) sumSST / n1;

		float Rsquared = 1 - (sumSSR / sumSST);

		String eq = "\n\nBeta Zero: " + BetaZero + "\nBeta One: " + BetaOne
				+ "\nBeta Two: " + BetaTwo + "\n\n R squared value is: "
				+ Rsquared;

		context.write(new Text("Beta Coefficients: "), new Text(eq));

	}
}
