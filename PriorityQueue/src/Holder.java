class Holder implements Comparable<Holder> {
	int value;
	boolean deleted;

	public Holder(int item) {
		this.value = item;
		this.deleted = false;
	}

	@Override
	public int compareTo(Holder h) {
		return h.value - this.value;
	}
}