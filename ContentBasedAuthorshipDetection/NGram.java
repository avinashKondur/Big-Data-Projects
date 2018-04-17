
import java.util.ArrayList;
import java.util.List;

public class NGram {

	private String[] _author;

	private List<String> _unigrams = null;

	private int _year;

	private List<String> _biGrams = null;

	private String DELIMETER = "<===>";
	private String LINE_SEP = " ";	
	private String _START_ = "\"_START_\"";
	private String _END_ = "\"_END_\"";

	public NGram(String value) {

		// .. taking to assumption the mapper sends only one line per request
		String[] values = value.toString().split(DELIMETER);

		this._author = values[0].toLowerCase().split(LINE_SEP);
		

		values[2] = values[2].replaceAll("[^A-Z0-9a-z\\s]", "");
		
		String[] array = values[2].split(LINE_SEP);
		
		this._unigrams = new ArrayList<String>();
		
		for(String s : array){			
			s = s.trim();			
			if(!s.isEmpty())
				this._unigrams.add(s.toLowerCase());
		}

		String[] date = values[1].split(LINE_SEP);

		this._year = Integer.parseInt(date[date.length - 1]);
	}

	public String[] GetAuthor() {
		return this._author;
	}
	
	public String GetAuthorLastName() {
		return this._author.length >= 2 ? this._author[this._author.length-1] : this._author[0];
	}

	public String GetAuthorFristName() {
		return this._author[0];
	}
	
	public int Getyear() {
		return this._year;
	}
	
	public List<String> GetUniGrams() {

		return this._unigrams;

	}

	public List<String> GetBiGrams() {

		if (this._biGrams == null) {

			this._biGrams = new ArrayList<String>();

			for (int i = 0; i < this._unigrams.size(); i++) {

				if (i == 0) {
					this._biGrams.add(this._START_ + "  " + this._unigrams.get(i));
				} else if (i == this._unigrams.size() - 1) {
					this._biGrams.add(this._unigrams.get(i) + "  " + this._END_);

				} else {

					this._biGrams.add(this._unigrams.get(i-1) + "  " + this._unigrams.get(i));

				}

			}
		}

		return this._biGrams;

	}	
}
