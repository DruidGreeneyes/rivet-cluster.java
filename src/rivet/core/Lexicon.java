package rivet.core;

import java.util.List;
import java.util.Optional;
import java.util.function.Function;

import rivet.Util;
import rivet.core.HashLabels;
import rivet.core.RIV;
import rivet.core.Metadata;
import rivet.core.Words;

public class Lexicon {
	private Metadata metadata;
	private Words words;
	private final RIV emptyRIV = new RIV();
	
	public Lexicon(Metadata metadata, Words words) {
		this.metadata = metadata;
		this.words = words;
	}
	
	public Words setWords(Words words) {
		this.words = words;
		return this.words;
	}
	
	public Words getWords() { return this.words; }
	public int   getSize()  { return this.metadata.getSize(); }
	public int   getK()     { return this.metadata.getK(); }
	public int   getCR()    { return this.metadata.getCR(); }
	
	public RIV getWordInd (String word) {
		return HashLabels.generateLabel(this.getSize(), this.getK(), word);
	}
	
	public RIV getWordLex (String word) {
		return (this.words.containsKey(word))
				? this.words.get(word)
				: this.getWordInd(word);
	}
	
	public Words trainWordFromContext (Words words, String word, List<String> context) {
		final Optional<RIV> sum = context.stream()
				        			.map((w) -> this.getWordInd(w))
				        			.reduce((c, x) -> HashLabels.addLabels(c, x));
		words.put(word, HashLabels.addLabels(this.getWordLex(word),
											(sum.isPresent())
											? sum.get()
											: emptyRIV));
		return words;
	}
	
	private Words trainWordsFromText (Words words, List<String> tokens, int index, int cr) {
		if (index >= tokens.size()) return words;
		words = trainWordFromContext(
						words,
						tokens.get(index),
						Util.getContextWindow(index, cr, tokens));
		return trainWordsFromText(words, tokens, index + 1, cr);
	}
	private Words trainWordsFromText (Words words, List<String> tokens) {
		return trainWordsFromText(words, tokens, 0, this.getCR());
	}
	public Words trainWordsFromText (List<String> tokenizedText) {
		return trainWordsFromText(Util.safeCopy(this.words), tokenizedText);
	}
	
	private Words trainWordsFromBatch (Words words, List<List<String>> tokenses) {
		tokenses.forEach((tokens)-> trainWordsFromText(words, tokens));
		return words;
	}
	public Words trainWordsFromBatch (List<List<String>> tokenizedTexts) {
		return trainWordsFromBatch (Util.safeCopy(this.words), tokenizedTexts);
	}
	
	public RIV lexDocument (List<String> tokenizedText) {
		Optional<RIV> res = tokenizedText.stream()
					.map((x) -> this.getWordLex(x))
					.reduce((c, x) -> HashLabels.addLabels(c, x));
		return (res.isPresent())
				? res.get()
				: emptyRIV;
	}
	
	public List<String> getPossibleTopicsForDocument (List<String> tokenizedText) {
		RIV document = this.lexDocument(tokenizedText);
		List<String> words = Util.setToList(this.words.keySet());
		Function<String, Double> keyfn = ((str) -> HashLabels.similarity(document, this.getWordLex(str)));
		return Util.takeBy(10, keyfn, words);
	}
}
