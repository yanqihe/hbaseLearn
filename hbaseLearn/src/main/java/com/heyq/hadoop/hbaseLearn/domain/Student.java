package com.heyq.hadoop.hbaseLearn.domain;

import java.io.Serializable;

public class Student implements Serializable {

	private static final long serialVersionUID = -3815405802567262714L;

	public static class Info {

		public Info() {
			super();
		}

		public Info(String studentId, String clazz) {
			super();
			this.studentId = studentId;
			this.clazz = clazz;
		}

		private String studentId;
		private String clazz;

		public String getStudentId() {
			return studentId;
		}

		public void setStudentId(String studentId) {
			this.studentId = studentId;
		}

		public String getClazz() {
			return clazz;
		}

		public void setClazz(String clazz) {
			this.clazz = clazz;
		}


		@Override
		public String toString() {
			return "Info [studentId=" + studentId + ", clazz=" + clazz + "]";
		}

	}

	public static class Score {

		public Score() {
			super();
		}

		public Score(int understanding, int programming) {
			super();
			this.understanding = understanding;
			this.programming = programming;
		}

		private int understanding;
		private int programming;

		public int getUnderstanding() {
			return understanding;
		}

		public void setUnderstanding(int understanding) {
			this.understanding = understanding;
		}

		public int getProgramming() {
			return programming;
		}

		public void setProgramming(int programming) {
			this.programming = programming;
		}

		@Override
		public String toString() {
			return "Score [understanding=" + understanding + ", programming=" + programming + "]";
		}
	}

	public Student() {
		super();
	}

	public Student(String studentName, Info info, Score score) {
		super();
		this.studentName = studentName;
		this.info = info;
		this.score = score;
	}

	/**
	 * 约定该属性是学生的唯一标识
	 */
	private String studentName;

	private Info info;

	private Score score;

	public String getStudentName() {
		return studentName;
	}

	public void setStudentName(String studentName) {
		this.studentName = studentName;
	}

	public Info getInfo() {
		return info;
	}

	public void setInfo(Info info) {
		this.info = info;
	}

	public Score getScore() {
		return score;
	}

	public void setScore(Score score) {
		this.score = score;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((studentName == null) ? 0 : studentName.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		Student other = (Student) obj;
		if (studentName == null) {
			if (other.studentName != null)
				return false;
		} else if (!studentName.equals(other.studentName))
			return false;
		return true;
	}

	@Override
	public String toString() {
		return "Student [studentName=" + studentName + ", info=" + info + ", score=" + score + "]";
	}
}
