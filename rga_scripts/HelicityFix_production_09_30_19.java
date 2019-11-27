import java.util.ArrayList;
import java.util.List;

import org.jlab.jnp.hipo4.data.Bank;
import org.jlab.jnp.hipo4.data.Event;
import org.jlab.jnp.hipo4.data.SchemaFactory;
import org.jlab.jnp.hipo4.io.*;
import org.jlab.utils.system.ClasUtilsFile;
import org.jlab.detector.decode.DaqScalers;
import org.jlab.detector.decode.DaqScalersSequence;
import org.jlab.detector.helicity.HelicityBit;
import org.jlab.detector.helicity.HelicitySequenceDelayed;
import org.jlab.detector.helicity.HelicitySequenceManager;

public class HelicityFix_production_09_30_19 {

	public static void main(String[] args) {
		String fileout = args[0];
		List<String> filenames = new ArrayList<>();
		for (int i = 1; i < args.length; i++)
			filenames.add(args[i]);

		//HelicitySequenceDelayed seq = HelicityAnalysis.readSequence(filenames);
        HelicitySequenceManager seq = new HelicitySequenceManager(8,filenames);
		//seq.setVerbosity(1);
		DaqScalersSequence chargeSequence = DaqScalersSequence.readSequence(filenames);

		HipoReader reader0 = new HipoReader();
		reader0.setTags(0, 1);
		reader0.open(args[1]);
		SchemaFactory factory = reader0.getSchemaFactory();

		HipoWriterSorted writer = new HipoWriterSorted();
		writer.getSchemaFactory().initFromDirectory(ClasUtilsFile.getResourceDir("COATJAVA", "etc/bankdefs/hipo4"));
		writer.setCompressionType(1);
		writer.open(fileout);

		for (String filename : filenames) {
			System.out.println(String.format(" >>>>>>>>>>>>>>>>>>>>>> NOW READING %s", filename));
			HipoReader reader = new HipoReader();
			reader.setTags(0, 1);
			reader.open(filename);

			Event event = new Event();
			Bank runConfigBank = new Bank(writer.getSchemaFactory().getSchema("RUN::config"));
			Bank runScalerBank = new Bank(writer.getSchemaFactory().getSchema("RUN::scaler"));
			Bank recParticleBank = new Bank(writer.getSchemaFactory().getSchema("REC::Particle"));
			Bank recEventBank = new Bank(writer.getSchemaFactory().getSchema("REC::Event"));

			int evtRead = 0, evtWrit = 0;
			int Nundf = 0;
			long badCharge = 0;
			long goodCharge = 0;
			
            while (reader.hasNext()) {

                evtRead++;
				reader.nextEvent(event);

				HelicityBit predicted = seq.search(event);

				DaqScalers ds = chargeSequence.get(event);
				//DaqScalers ds = chargeSequence.get(timestamp);

                event.read(recEventBank);
					event.remove(recEventBank.getSchema());
				recEventBank.putFloat("beamCharge", 0, ds.getBeamCharge()); // Flipped for Fall 2018
				
                if (predicted == null || predicted == HelicityBit.UDF) {
					Nundf++;
					event.read(recEventBank);
					event.remove(recEventBank.getSchema());
					if (ds == null) {
						badCharge++;
					} else {
						goodCharge++;
						// do something useful with beam charge here:
						// +ds.getBeamCharge()+" "+ds.getBeamChargeGated());
						recEventBank.putFloat("beamCharge", 0, ds.getBeamCharge()); // Flipped for Fall 2018
					}
					event.write(recEventBank);
				} else {
					event.read(recParticleBank);
					boolean isWritten = false;

					evtWrit++;
					event.read(recEventBank);
					event.remove(recEventBank.getSchema());
					if (predicted.value() == 1) {
						recEventBank.putByte("helicity", 0, (byte) 1);
					} else if (predicted.value() == -1) {
						recEventBank.putByte("helicity", 0, (byte) -1);
					}

					if (ds == null) {
						badCharge++;
					} else {
						goodCharge++;
						// do something useful with beam charge here:
						// +ds.getBeamCharge()+" "+ds.getBeamChargeGated());
						recEventBank.putFloat("beamCharge", 0, ds.getBeamCharge()); // Flipped for Fall 2018
					}
					if (evtRead % 50000 == 0){
						System.out.println("recEventBank beamCharge: " + recEventBank.getFloat("beamCharge", 0));
					}

					event.write(recEventBank);
					event.read(recEventBank);
					if (evtRead % 50000 == 0) {
						System.out.println("recEventBank beamCharge after: " + recEventBank.getFloat("beamCharge", 0));
						Bank recEventBank2 = new Bank(factory.getSchema("REC::Event"));
						event.read(recEventBank2);
						System.out.println("recEventBank beamCharge after: " + recEventBank2.getFloat("beamCharge", 0));
					}

					if (evtRead % 50000 == 0) {
						System.out.println(filename + " -> " + fileout + " ; event = " + eventNumber + " : time " + timestamp + " : HEL " + predicted + " good/bad: " + goodCharge
								+ "/" + badCharge + " " + ((float) badCharge / (float) (goodCharge + badCharge)) * 100.0);
						float frcTot = (float) 100.0f * evtWrit / (1f * evtRead);
						float frcundf = (float) 100.0 * Nundf / (1f * evtRead);
						String messfrc = String.format(" ; f=%1.1f%% ; UDF = %1.1f%%", frcTot, frcundf);
						System.out.println("Read " + evtRead + " ; Written " + evtWrit + messfrc);
					}
				}
				writer.addEvent(event, event.getEventTag());

			}
			reader.close();
		}
		writer.close();
	}
}
